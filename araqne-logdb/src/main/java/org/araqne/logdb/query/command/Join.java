package org.araqne.logdb.query.command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.LogMap;
import org.araqne.logdb.LogQuery;
import org.araqne.logdb.LogQueryCommand;
import org.araqne.logdb.LogResultSet;
import org.araqne.logdb.query.command.Sort.SortField;
import org.araqne.logdb.query.engine.LogQueryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Join extends LogQueryCommand {
	public enum JoinType {
		Inner, Left
	}

	private final Logger logger = LoggerFactory.getLogger(Join.class);
	private final JoinType joinType;
	private Result subQueryResult;
	private LogResultSet subQueryResultSet;
	private volatile boolean subQueryEnd = false;

	// for later sort-merge join
	private Object[] sortJoinKeys1;
	private Object[] sortJoinKeys2;

	// for hash join
	private HashMap<JoinKeys, List<Object>> hashJoinMap;
	private JoinKeys joinKeys;

	private int joinKeyCount;
	private SortField[] sortFields;
	private String subQueryString;
	private List<LogQueryCommand> subQueryCommands;

	private SubQueryRunner subQueryRunner = new SubQueryRunner();

	public Join(JoinType joinType, SortField[] sortFields, String subQueryString, List<LogQueryCommand> subQueryCommands) {
		try {
			this.joinType = joinType;
			this.joinKeyCount = sortFields.length;
			this.joinKeys = new JoinKeys(new Object[joinKeyCount]);
			this.sortJoinKeys1 = new Object[sortFields.length];
			this.sortJoinKeys2 = new Object[sortFields.length];

			this.sortFields = sortFields;
			this.subQueryCommands = subQueryCommands;
			this.subQueryString = subQueryString;
			this.subQueryResult = new Result("sub");

			Sort sort = new Sort(null, sortFields);
			sort.init();

			LogQueryCommand lastCmd = subQueryCommands.get(subQueryCommands.size() - 1);

			lastCmd.setNextCommand(sort);
			sort.setNextCommand(subQueryResult);

			this.subQueryCommands.add(sort);
			this.subQueryCommands.add(subQueryResult);
		} catch (IOException e) {
			throw new IllegalStateException("cannot create join query", e);
		}
	}

	@Override
	public void setLogQuery(LogQuery logQuery) {
		LogQuery q = new LogQueryImpl(logQuery.getContext(), subQueryString, subQueryCommands);

		for (LogQueryCommand cmd : subQueryCommands)
			cmd.setLogQuery(q);

		super.setLogQuery(logQuery);
	}

	public JoinType getType() {
		return joinType;
	}

	public SortField[] getSortFields() {
		return sortFields;
	}

	public List<LogQueryCommand> getSubQuery() {
		return subQueryCommands;
	}

	@Override
	public void start() {
		Thread t = new Thread(subQueryRunner);
		t.start();
	}

	@Override
	public void push(LogMap m) {
		LogQueryCommand cmd = subQueryCommands.get(subQueryCommands.size() - 1);

		// wait until subquery end
		synchronized (cmd) {
			while (!subQueryEnd) {
				try {
					cmd.wait(1000);
				} catch (InterruptedException e) {
				}
			}
		}

		if (subQueryResultSet == null) {
			eof(true);
			return;
		}

		subQueryResultSet.reset();

		if (hashJoinMap != null) {
			int i = 0;
			for (SortField f : sortFields) {
				Object joinValue = m.get(f.getName());
				if (joinValue instanceof Integer || joinValue instanceof Short)
					joinValue = ((Number) joinValue).longValue();
				joinKeys.keys[i++] = joinValue;
			}

			List<Object> l = hashJoinMap.get(joinKeys);
			if (l == null) {
				if (joinType == JoinType.Left)
					write(m);
				return;
			}

			for (Object o : l) {
				@SuppressWarnings("unchecked")
				Map<String, Object> sm = (Map<String, Object>) o;
				Map<String, Object> joinMap = new HashMap<String, Object>(m.map());
				joinMap.putAll(sm);
				write(new LogMap(joinMap));
			}
			return;
		}

		int i = 0;
		for (SortField f : sortFields) {
			Object joinValue = m.get(f.getName());
			if (joinValue instanceof Integer || joinValue instanceof Short)
				joinValue = ((Number) joinValue).longValue();
			sortJoinKeys1[i++] = joinValue;
		}

		boolean found = false;
		while (subQueryResultSet.hasNext()) {
			Map<String, Object> sm = subQueryResultSet.next();

			i = 0;
			for (SortField f : sortFields) {
				Object joinValue = sm.get(f.getName());
				if (joinValue instanceof Integer || joinValue instanceof Short)
					joinValue = ((Number) joinValue).longValue();
				sortJoinKeys2[i++] = joinValue;
			}

			if (Arrays.equals(sortJoinKeys1, sortJoinKeys2)) {
				Map<String, Object> joinMap = new HashMap<String, Object>(m.map());
				joinMap.putAll(sm);
				write(new LogMap(joinMap));
				found = true;
			}
		}

		if (joinType == JoinType.Left && !found)
			write(m);
	}

	@Override
	public boolean isReducer() {
		return false;
	}

	@Override
	public void eof(boolean cancelled) {
		subQueryCommands.get(0).eof(cancelled);

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
		private static final int HASH_JOIN_THRESHOLD = 50000;

		@Override
		public void run() {
			boolean completed = false;
			LogQueryCommand cmd = null;
			try {
				cmd = subQueryCommands.get(subQueryCommands.size() - 1);
				for (int i = subQueryCommands.size() - 1; i >= 0; i--)
					subQueryCommands.get(i).start();

				subQueryCommands.get(0).eof(false);
				completed = true;

				try {
					subQueryResultSet = subQueryResult.getResult();
					logger.debug("araqne logdb: fetch subquery result of query [{}:{}]", logQuery.getId(), logQuery.getQueryString());

					if (subQueryResultSet.size() <= HASH_JOIN_THRESHOLD)
						buildHashJoinTable();

				} catch (IOException e) {
					logger.error("araqne logdb: cannot get subquery result of query " + logQuery.getId(), e);
				}

			} catch (Throwable t) {
				if (!completed)
					subQueryCommands.get(0).eof(true);

				logger.error("araqne logdb: subquery failed, query " + logQuery.getId(), t);
			} finally {
				subQueryEnd = true;

				if (cmd != null) {
					synchronized (cmd) {
						cmd.notifyAll();
					}
				}

				logger.debug("araqne logdb: subquery end, query " + logQuery.getId());
			}
		}

		private void buildHashJoinTable() {
			hashJoinMap = new HashMap<JoinKeys, List<Object>>(50000);

			while (subQueryResultSet.hasNext()) {
				Map<String, Object> sm = subQueryResultSet.next();

				Object[] keys = new Object[joinKeyCount];
				for (int i = 0; i < joinKeyCount; i++) {
					Object joinValue = sm.get(sortFields[i].getName());
					if (joinValue instanceof Integer || joinValue instanceof Short) {
						joinValue = ((Number) joinValue).longValue();
					}
					keys[i] = joinValue;
				}

				JoinKeys joinKeys = new JoinKeys(keys);
				List<Object> l = hashJoinMap.get(joinKeys);
				if (l == null) {
					l = new ArrayList<Object>(2);
					hashJoinMap.put(joinKeys, l);
				}

				l.add(sm);
			}
		}
	}

	private static class JoinKeys {
		private Object[] keys;

		public JoinKeys(Object[] keys) {
			this.keys = keys;
		}

		@Override
		public int hashCode() {
			return Arrays.hashCode(keys);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			JoinKeys other = (JoinKeys) obj;
			if (!Arrays.equals(keys, other.keys))
				return false;
			return true;
		}
	}
}
