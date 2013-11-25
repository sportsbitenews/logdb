package org.araqne.logdb.query.command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.Row;
import org.araqne.logdb.Query;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryResultSet;
import org.araqne.logdb.RowPipe;
import org.araqne.logdb.query.command.Sort.SortField;
import org.araqne.logdb.query.engine.QueryTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Join extends QueryCommand {
	public enum JoinType {
		Inner, Left
	}

	private final Logger logger = LoggerFactory.getLogger(Join.class);
	private final JoinType joinType;
	private QueryResultSet subQueryResultSet;

	// for later sort-merge join
	private Object[] sortJoinKeys1;
	private Object[] sortJoinKeys2;

	// for hash join
	private HashMap<JoinKeys, List<Object>> hashJoinMap;
	private JoinKeys joinKeys;

	private int joinKeyCount;
	private SortField[] sortFields;

	private Query subQuery;

	// tasks
	private SubQueryTask subQueryTask = new SubQueryTask();

	public Join(JoinType joinType, SortField[] sortFields, Query subQuery) {
		this.joinType = joinType;
		this.joinKeyCount = sortFields.length;
		this.joinKeys = new JoinKeys(new Object[joinKeyCount]);
		this.sortJoinKeys1 = new Object[sortFields.length];
		this.sortJoinKeys2 = new Object[sortFields.length];
		this.sortFields = sortFields;
		this.subQuery = subQuery;

		for (QueryCommand cmd : subQuery.getCommands()) {
			if (cmd.getMainTask() != null) {
				subQueryTask.addDependency(cmd.getMainTask());
				subQueryTask.addSubTask(cmd.getMainTask());
			}
		}
	}

	@Override
	public void onPush(Row m) {
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
					pushPipe(m);
				return;
			}

			for (Object o : l) {
				@SuppressWarnings("unchecked")
				Map<String, Object> sm = (Map<String, Object>) o;
				Map<String, Object> joinMap = new HashMap<String, Object>(m.map());
				joinMap.putAll(sm);
				pushPipe(new Row(joinMap));
			}
			return;
		}

		subQueryResultSet.reset();
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
				pushPipe(new Row(joinMap));
				found = true;
			}
		}

		if (joinType == JoinType.Left && !found)
			pushPipe(m);
	}

	@Override
	public QueryTask getMainTask() {
		return subQueryTask;
	}

	public JoinType getType() {
		return joinType;
	}

	public SortField[] getSortFields() {
		return sortFields;
	}

	public List<QueryCommand> getSubQuery() {
		return subQuery.getCommands();
	}

	// bulid hash table or sort
	private class SubQueryTask extends QueryTask {
		private static final int HASH_JOIN_THRESHOLD = 50000;

		@Override
		public void run() {
			logger.debug("araqne logdb: subquery end, query " + query.getId());

			try {
				subQuery.postRun();

				QueryResultSet rs = subQuery.getResultSet();

				logger.debug("araqne logdb: fetch subquery result of query [{}:{}]", query.getId(), query.getQueryString());

				if (rs.size() <= HASH_JOIN_THRESHOLD)
					buildHashJoinTable(rs);
				else
					subQueryResultSet = rs;

			} catch (IOException e) {
				logger.error("araqne logdb: cannot get subquery result of query " + query.getId(), e);
			}

		}

		private void buildHashJoinTable(QueryResultSet rs) {
			hashJoinMap = new HashMap<JoinKeys, List<Object>>(HASH_JOIN_THRESHOLD);

			while (rs.hasNext()) {
				Map<String, Object> sm = rs.next();

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

		@Override
		public RowPipe getOutput() {
			return null;
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