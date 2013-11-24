package org.araqne.logdb.query.command;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.araqne.log.api.LogParser;
import org.araqne.log.api.LogParserFactory;
import org.araqne.log.api.LogParserInput;
import org.araqne.log.api.LogParserOutput;
import org.araqne.log.api.LoggerConfigOption;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.Row;
import org.araqne.logdb.Permission;
import org.araqne.logdb.RowPipe;
import org.araqne.logdb.QueryCommand.Status;
import org.araqne.logdb.query.command.Table.TableParams;
import org.araqne.logdb.query.engine.QueryTask;
import org.araqne.logstorage.Log;
import org.araqne.logstorage.LogSearchCallback;
import org.araqne.logstorage.TableWildcardMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableScanTask extends QueryTask {
	private final Logger logger = LoggerFactory.getLogger(TableScanTask.class);

	private Table cmd;
	private TableParams params;
	private volatile boolean stopped;

	public TableScanTask(Table cmd, TableParams params) {
		this.cmd = cmd;
		this.params = params;
	}

	@Override
	public RowPipe getOutput() {
		return cmd.getOutput();
	}

	public TableParams getParams() {
		return params;
	}

	public void setParams(TableParams params) {
		this.params = params;
	}

	@Override
	public void run() {
		long found = 0;

		try {
			for (String tableName : expandTableNames(params.getTableNames())) {
				String parserName = cmd.getTableRegistry().getTableMetadata(tableName, "parser");

				// override parser
				if (params.getParserName() != null)
					parserName = params.getParserName();

				String parserFactoryName = cmd.getTableRegistry().getTableMetadata(tableName, "logparser");

				LogParser parser = null;
				if (parserName != null && cmd.getParserRegistry().getProfile(parserName) != null) {
					try {
						parser = cmd.getParserRegistry().newParser(parserName);
					} catch (IllegalStateException e) {
						if (logger.isDebugEnabled())
							logger.debug("araqne logdb: cannot create parser [{}], skipping", parserName);
					}
				}

				LogParserFactory parserFactory = cmd.getParserFactoryRegistry().get(parserFactoryName);
				if (parser == null && parserFactory != null) {
					Map<String, String> prop = new HashMap<String, String>();
					for (LoggerConfigOption configOption : parserFactory.getConfigOptions()) {
						String optionName = configOption.getName();
						String optionValue = cmd.getTableRegistry().getTableMetadata(tableName, optionName);
						if (configOption.isRequired() && optionValue == null)
							throw new IllegalArgumentException("require table metadata " + optionName);
						if (optionValue != null)
							prop.put(optionName, optionValue);
					}
					parser = parserFactory.createParser(prop);
				}

				long needed = params.getLimit() - found;
				if (params.getLimit() != 0 && needed <= 0)
					break;

				LogSearchCallback searchCallback = null;
				if (parser != null && parser.getVersion() == 2)
					searchCallback = new LogSearchCallbackV2(parser);
				else
					searchCallback = new LogSearchCallbackV1(parser);

				found += cmd.getStorage().search(tableName, params.getFrom(), params.getTo(), params.getOffset(),
						params.getLimit() == 0 ? 0 : needed, searchCallback);
				if (params.getOffset() > 0) {
					if (found > params.getOffset()) {
						found -= params.getOffset();
						params.setOffset(0);
					} else {
						params.setOffset(params.getOffset() - found);
						found = 0;
					}
				}
			}
		} catch (InterruptedException e) {
			logger.trace("araqne logdb: query interrupted");
		} catch (Exception e) {
			logger.error("araqne logdb: table exception", e);
		} catch (Error e) {
			logger.error("araqne logdb: table error", e);
		} finally {
			cmd.setStatus(Status.End);
		}
	}

	public void stop() {
		stopped = true;
	}

	private List<String> expandTableNames(List<String> tableNames) {
		List<String> localTableNames = new ArrayList<String>();
		for (String s : tableNames) {
			if (s.contains("*"))
				localTableNames.addAll(matchTables(s));
			else if (isAccessible(s))
				localTableNames.add(s);
		}
		return localTableNames;
	}

	private List<String> matchTables(String tableNameExpr) {
		List<String> filtered = new ArrayList<String>();
		for (String name : TableWildcardMatcher.apply(new HashSet<String>(cmd.getTableRegistry().getTableNames()), tableNameExpr)) {
			if (!isAccessible(name))
				continue;

			filtered.add(name);
		}
		return filtered;
	}

	private boolean isAccessible(String name) {
		return cmd.getAccountService().checkPermission(cmd.getContext().getSession(), name, Permission.READ);
	}

	private class LogSearchCallbackV1 implements LogSearchCallback {
		private boolean suppressBugAlert = false;
		private LogParser parser;

		public LogSearchCallbackV1(LogParser parser) {
			this.parser = parser;
		}

		@Override
		public void onLog(Log log) {
			Map<String, Object> m = null;

			Map<String, Object> data = log.getData();
			if (parser != null) {
				try {
					data.put("_time", log.getDate());
					Map<String, Object> parsed = parser.parse(data);
					if (parsed != null) {
						parsed.put("_table", log.getTableName());
						parsed.put("_id", log.getId());

						Object time = parsed.get("_time");
						if (time == null)
							parsed.put("_time", log.getDate());
						else if (!(time instanceof Date)) {
							logger.error("araqne logdb: parser returned wrong _time type: " + time.getClass().getName());
							cmd.getQuery().stop(QueryStopReason.CommandFailure);
						}

						m = parsed;
					} else {
						logger.debug("araqne logdb: cannot parse log [{}]", data);
						return;
					}
				} catch (Throwable t) {
					if (!suppressBugAlert) {
						logger.error(
								"araqne logdb: PARSER BUG! original log => table " + log.getTableName() + ", id " + log.getId()
										+ ", data " + data, t);
						suppressBugAlert = true;
					}

					// can be unmodifiableMap when it comes from memory buffer.
					m = new HashMap<String, Object>();
					m.putAll(data);
					m.put("_table", log.getTableName());
					m.put("_id", log.getId());
					m.put("_time", log.getDate());
				}
			} else {
				// can be unmodifiableMap when it comes from memory buffer.
				m = new HashMap<String, Object>();
				m.putAll(data);
				m.put("_table", log.getTableName());
				m.put("_id", log.getId());
				m.put("_time", log.getDate());
			}

			cmd.onPush(new Row(m));
		}

		@Override
		public void interrupt() {
		}

		@Override
		public boolean isInterrupted() {
			return stopped;
		}
	}

	private class LogSearchCallbackV2 implements LogSearchCallback {
		private boolean suppressBugAlert = false;
		private final LogParser parser;
		private final LogParserInput input = new LogParserInput();

		public LogSearchCallbackV2(LogParser parser) {
			this.parser = parser;
		}

		@Override
		public void onLog(Log log) {
			input.setDate(log.getDate());
			input.setSource(log.getTableName());
			input.setData(log.getData());

			try {
				LogParserOutput output = parser.parse(input);
				if (output != null) {
					for (Map<String, Object> row : output.getRows()) {
						row.put("_table", log.getTableName());
						row.put("_id", log.getId());

						Object time = row.get("_time");
						if (time == null)
							row.put("_time", log.getDate());
						else if (!(time instanceof Date)) {
							logger.error("araqne logdb: parser returned wrong _time type: " + time.getClass().getName());
							cmd.getQuery().stop(QueryStopReason.CommandFailure);
						}

						if (output != null)
							cmd.onPush(new Row(row));
					}

				} else {
					logger.debug("araqne logdb: cannot parse log [{}]", log.getData());
					return;
				}
			} catch (Throwable t) {
				if (!suppressBugAlert) {
					logger.error("araqne logdb: PARSER BUG! original log => table " + log.getTableName() + ", id " + log.getId()
							+ ", data " + log.getData(), t);
					suppressBugAlert = true;
				}

				// NOTE: log can be unmodifiableMap when it comes from memory
				// buffer.
				HashMap<String, Object> row = new HashMap<String, Object>(log.getData());
				row.put("_table", log.getTableName());
				row.put("_id", log.getId());
				row.put("_time", log.getDate());

				cmd.onPush(new Row(row));
			}
		}

		@Override
		public void interrupt() {
		}

		@Override
		public boolean isInterrupted() {
			return stopped;
		}
	}
}
