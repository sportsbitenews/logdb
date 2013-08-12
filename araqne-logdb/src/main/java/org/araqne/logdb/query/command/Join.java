package org.araqne.logdb.query.command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.LogMap;
import org.araqne.logdb.LogQueryCommand;
import org.araqne.logdb.LogResultSet;
import org.araqne.logdb.query.command.Sort.SortField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Join extends LogQueryCommand {
	private final Logger logger = LoggerFactory.getLogger(Join.class);

	private Result subQueryResult;
	private LogResultSet subQueryResultSet;

	private SortField[] sortFields;
	private List<LogQueryCommand> subQuery;

	private SubQueryRunner subQueryRunner = new SubQueryRunner();

	public Join(SortField[] sortFields, List<LogQueryCommand> subQuery) {
		try {
			this.sortFields = sortFields;
			this.subQuery = subQuery;
			this.subQueryResult = new Result("sub");

			Sort sort = new Sort(sortFields);
			sort.init();

			LogQueryCommand lastCmd = subQuery.get(subQuery.size() - 1);

			lastCmd.setNextCommand(sort);
			sort.setNextCommand(subQueryResult);

			this.subQuery.add(sort);
			this.subQuery.add(subQueryResult);
		} catch (IOException e) {
			throw new IllegalStateException("cannot create join query", e);
		}
	}

	public SortField[] getSortFields() {
		return sortFields;
	}

	public List<LogQueryCommand> getSubQuery() {
		return subQuery;
	}

	@Override
	public void start() {
		Thread t = new Thread(subQueryRunner);
		t.start();
	}

	@Override
	public void push(LogMap m) {
		LogQueryCommand cmd = subQuery.get(subQuery.size() - 1);

		// wait until subquery end
		synchronized (cmd) {
			while (subQueryResultSet == null && cmd.getStatus() != Status.End) {
				try {
					cmd.wait(1000);
				} catch (InterruptedException e) {
				}
			}
		}

		if (subQueryResultSet == null) {
			try {
				subQueryResultSet = subQueryResult.getResult();
				logger.debug("araqne logdb: fetch subquery result of query [{}:{}]", logQuery.getId(), logQuery.getQueryString());
			} catch (IOException e) {
				logger.error("araqne logdb: cannot get subquery result of query " + logQuery.getId(), e);
			}
		} else {
			subQueryResultSet.reset();
		}

		ArrayList<Object> joinKeys = new ArrayList<Object>();
		for (SortField f : sortFields)
			joinKeys.add(m.get(f.getName()));

		while (subQueryResultSet.hasNext()) {
			Map<String, Object> sm = subQueryResultSet.next();
			int i = 0;
			for (SortField f : sortFields) {
				Object v1 = sm.get(f.getName());
				Object v2 = joinKeys.get(i);

				if (v1 == null && v2 == null) {
					Map<String, Object> joinMap = new HashMap<String, Object>(m.map());
					joinMap.putAll(sm);
					write(new LogMap(joinMap));
				} else if (v1 != null && v2 != null && v1.equals(v2)) {
					Map<String, Object> joinMap = new HashMap<String, Object>(m.map());
					joinMap.putAll(sm);
					write(new LogMap(joinMap));
				}
			}
		}
	}

	@Override
	public boolean isReducer() {
		return false;
	}

	@Override
	public void eof(boolean cancelled) {
		subQuery.get(0).eof(cancelled);

		if (subQueryResultSet != null) {
			try {
				subQueryResultSet.close();
			} catch (Throwable t) {
				logger.error("araqne logdb: subquery result set close failed, query " + logQuery.getId(), t);
			}
		}

		if (subQueryResult != null) {
			try {
				subQueryResult.purge();
			} catch (Throwable t) {
				logger.error("araqne logdb: subquery result purge failed, query " + logQuery.getId(), t);
			}
		}

		super.eof(cancelled);
	}

	private class SubQueryRunner implements Runnable {
		@Override
		public void run() {
			try {
				for (int i = subQuery.size() - 1; i >= 0; i--)
					subQuery.get(i).start();

				subQuery.get(0).eof(false);
				LogQueryCommand cmd = subQuery.get(subQuery.size() - 1);

				synchronized (cmd) {
					cmd.notifyAll();
				}
			} catch (Throwable t) {
				logger.error("araqne logdb: subquery failed, query " + logQuery.getId(), t);
			} finally {
				logger.debug("araqne logdb: subquery end, query " + logQuery.getId());
			}
		}
	}

}
