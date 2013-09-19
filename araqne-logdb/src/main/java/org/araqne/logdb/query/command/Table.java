/*
 * Copyright 2011 Future Systems
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.araqne.logdb.query.command;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.log.api.LogParser;
import org.araqne.log.api.LogParserFactory;
import org.araqne.log.api.LogParserFactoryRegistry;
import org.araqne.log.api.LogParserInput;
import org.araqne.log.api.LogParserOutput;
import org.araqne.log.api.LogParserRegistry;
import org.araqne.log.api.LoggerConfigOption;
import org.araqne.logdb.LogMap;
import org.araqne.logdb.LogQueryCommand;
import org.araqne.logstorage.Log;
import org.araqne.logstorage.LogSearchCallback;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogTableRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Table extends LogQueryCommand {
	private final Logger logger = LoggerFactory.getLogger(Table.class);
	private LogStorage storage;
	private LogTableRegistry tableRegistry;
	private LogParserFactoryRegistry parserFactoryRegistry;
	private LogParserRegistry parserRegistry;

	private TableParams params = new TableParams();
	private long found;

	public Table(TableParams params) {
		this.params = params;
	}

	public List<String> getTableNames() {
		return params.tableNames;
	}

	public void setTableNames(List<String> tableNames) {
		params.tableNames = tableNames;
	}

	public LogStorage getStorage() {
		return storage;
	}

	public void setStorage(LogStorage storage) {
		this.storage = storage;
	}

	public LogTableRegistry getTableRegistry() {
		return tableRegistry;
	}

	public void setTableRegistry(LogTableRegistry tableRegistry) {
		this.tableRegistry = tableRegistry;
	}

	public LogParserFactoryRegistry getParserFactoryRegistry() {
		return parserFactoryRegistry;
	}

	public void setParserFactoryRegistry(LogParserFactoryRegistry parserFactoryRegistry) {
		this.parserFactoryRegistry = parserFactoryRegistry;
	}

	public LogParserRegistry getParserRegistry() {
		return parserRegistry;
	}

	public void setParserRegistry(LogParserRegistry parserRegistry) {
		this.parserRegistry = parserRegistry;
	}

	public long getOffset() {
		return params.offset;
	}

	public void setOffset(long offset) {
		params.offset = offset;
	}

	public long getLimit() {
		return params.limit;
	}

	public void setLimit(long limit) {
		params.limit = limit;
	}

	public Date getFrom() {
		return params.from;
	}

	public Date getTo() {
		return params.to;
	}

	@Override
	public void start() {
		try {
			status = Status.Running;

			for (String tableName : params.tableNames) {
				String parserName = tableRegistry.getTableMetadata(tableName, "parser");

				// override parser
				if (params.parserName != null)
					parserName = params.parserName;

				String parserFactoryName = tableRegistry.getTableMetadata(tableName, "logparser");

				LogParser parser = null;
				if (parserName != null && parserRegistry.getProfile(parserName) != null) {
					try {
						parser = parserRegistry.newParser(parserName);
					} catch (IllegalStateException e) {
						if (logger.isDebugEnabled())
							logger.debug("araqne logdb: cannot create parser [{}], skipping", parserName);
					}
				}

				LogParserFactory parserFactory = parserFactoryRegistry.get(parserFactoryName);
				if (parser == null && parserFactory != null) {
					Map<String, String> prop = new HashMap<String, String>();
					for (LoggerConfigOption configOption : parserFactory.getConfigOptions()) {
						String optionName = configOption.getName();
						String optionValue = tableRegistry.getTableMetadata(tableName, optionName);
						if (configOption.isRequired() && optionValue == null)
							throw new IllegalArgumentException("require table metadata " + optionName);
						if (optionValue != null)
							prop.put(optionName, optionValue);
					}
					parser = parserFactory.createParser(prop);
				}

				long needed = params.limit - found;
				if (params.limit != 0 && needed <= 0)
					break;

				LogSearchCallback searchCallback = null;
				if (parser != null && parser.getVersion() == 2)
					searchCallback = new LogSearchCallbackV2(parser);
				else
					searchCallback = new LogSearchCallbackV1(parser);

				found += storage.search(tableName, params.from, params.to, params.offset, params.limit == 0 ? 0 : needed, searchCallback);
				if (params.offset > 0) {
					if (found > params.offset) {
						found -= params.offset;
						params.offset = 0;
					} else {
						params.offset -= found;
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
			eof(false);
		}
	}

	@Override
	public void push(LogMap m) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isReducer() {
		return false;
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
							eof(true);
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

			write(new LogMap(m));
		}

		@Override
		public void interrupt() {
			eof(true);
		}

		@Override
		public boolean isInterrupted() {
			return status.equals(Status.End);
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
							eof(true);
						}

						write(new LogMap(row));
					}

				} else {
					logger.debug("araqne logdb: cannot parse log [{}]", log.getData());
					return;
				}
			} catch (Throwable t) {
				if (!suppressBugAlert) {
					logger.error(
							"araqne logdb: PARSER BUG! original log => table " + log.getTableName() + ", id " + log.getId()
									+ ", data " + log.getData(), t);
					suppressBugAlert = true;
				}

				// NOTE: log can be unmodifiableMap when it comes from memory buffer.
				HashMap<String, Object> row = new HashMap<String, Object>(log.getData());
				row.put("_table", log.getTableName());
				row.put("_id", log.getId());
				row.put("_time", log.getDate());
				write(new LogMap(row));
			}
		}

		@Override
		public void interrupt() {
			eof(true);
		}

		@Override
		public boolean isInterrupted() {
			return status.equals(Status.End);
		}
	}

	public static class TableParams {
		private List<String> tableNames;
		private long offset;
		private long limit;
		private Date from;
		private Date to;
		private String parserName;

		public List<String> getTableNames() {
			return tableNames;
		}

		public void setTableNames(List<String> tableNames) {
			this.tableNames = tableNames;
		}

		public long getOffset() {
			return offset;
		}

		public void setOffset(long offset) {
			this.offset = offset;
		}

		public long getLimit() {
			return limit;
		}

		public void setLimit(long limit) {
			this.limit = limit;
		}

		public Date getFrom() {
			return from;
		}

		public void setFrom(Date from) {
			this.from = from;
		}

		public Date getTo() {
			return to;
		}

		public void setTo(Date to) {
			this.to = to;
		}

		public String getParserName() {
			return parserName;
		}

		public void setParserName(String parserName) {
			this.parserName = parserName;
		}

	}
}
