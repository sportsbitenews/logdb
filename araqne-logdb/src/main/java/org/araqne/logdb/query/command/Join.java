package org.araqne.logdb.query.command;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.Query;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryResultSet;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.QueryTask;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.impl.QueryHelper;
import org.araqne.logdb.query.command.Sort.SortField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Join extends QueryCommand {
	public enum JoinType {
		Inner, Left, Right, Full
	}

	private final Logger logger = LoggerFactory.getLogger(Join.class);
	private final JoinType joinType;

	// for hash join
	private HashMap<JoinKeys, List<Object>> hashJoinMap;
	private JoinKeys joinKeys;

	private int joinKeyCount;
	private SortField[] sortFields;

	private Query subQuery;

	// tasks
	private SubQueryTask subQueryTask = new SubQueryTask();

	private SortMergeJoiner sortMergeJoiner;

	public Join(JoinType joinType, SortField[] sortFields, Query subQuery) {
		this.joinType = joinType;
		this.joinKeyCount = sortFields.length;
		this.joinKeys = new JoinKeys(new Object[joinKeyCount]);
		this.sortFields = sortFields;
		this.subQuery = subQuery;
		this.sortMergeJoiner = new SortMergeJoiner(joinType, sortFields, new SortMergeJoinerCallback(this));

		logger.debug("araqne logdb: join subquery created [{}:{}]", subQuery.getId(), subQuery.getQueryString());
	}

	@Override
	public String getName() {
		return "join";
	}

	@Override
	public void onStart() {
		QueryHelper.setJoinAndUnionDependencies(subQuery.getCommands());

		for (QueryCommand cmd : subQuery.getCommands()) {
			if (cmd.getMainTask() != null) {
				subQueryTask.addDependency(cmd.getMainTask());
				subQueryTask.addSubTask(cmd.getMainTask());
			}
		}

		subQuery.preRun();
	}

	@Override
	public void onClose(QueryStopReason reason) {
		if (hashJoinMap != null) {
			hashJoinMap = null;
		} else {
			if (reason == QueryStopReason.PartialFetch || reason == QueryStopReason.End) {
				sortMergeJoiner.merge();
			} else {
				try {
					sortMergeJoiner.cancel();
				} catch (Throwable t) {
					logger.error("araqne logdb: can not cancel sortMergeJoiner", t);
				}
			}
		}

		try {
			subQuery.cancel(reason);
		} catch (Throwable t) {
			logger.error("araqne logdb: cannot stop subquery [" + subQuery.getQueryString() + "]", t);
		} finally {
			subQuery.purge();
		}
	}

	@Override
	public void onPush(RowBatch rowBatch) {
		if (rowBatch.selectedInUse) {
			for (int i = 0; i < rowBatch.size; i++) {
				Row row = rowBatch.rows[rowBatch.selected[i]];
				onPush(row);
			}
		} else {
			for (int i = 0; i < rowBatch.size; i++) {
				Row row = rowBatch.rows[i];
				onPush(row);
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
		} else {
			try {
				sortMergeJoiner.setR(m);
			} catch (Throwable t) {
				logger.error("araqne logdb: cannot setR on sortMergeJoiner[" + m.toString() + "]", t);
			}
		}
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

	public Query getSubQuery() {
		return subQuery;
	}

	@Override
	public String toString() {
		String typeOpt = "";
		if (joinType == JoinType.Left)
			typeOpt = " type=left";

		return "join" + typeOpt + " " + SortField.serialize(sortFields) + " [ " + subQuery.getQueryString() + " ] ";
	}

	// bulid hash table or sort
	private class SubQueryTask extends QueryTask {
		private final int HASH_JOIN_THRESHOLD = Integer.parseInt(System.getProperty("araqne.hashjointhreshold", "100000"));

		@Override
		public void run() {
			logger.debug("araqne logdb: join subquery end, main query [{}] sub query [{}]", query.getId(), subQuery.getId());

			QueryResultSet rs = null;
			try {
				subQuery.postRun();

				rs = subQuery.getResultSet();

				logger.debug(
						"araqne logdb: join fetch subquery result of query [{}:{}]", query.getId(),
						query.getQueryString());

				if (rs.size() <= HASH_JOIN_THRESHOLD && (joinType == JoinType.Inner || joinType == JoinType.Left))
					buildHashJoinTable(rs);
				else
					sortMergeJoiner.setS(rs);

			} catch (Throwable e) {
				logger.error("araqne logdb: cannot get subquery result of query " + query.getId(), e);
			} finally {
				if (rs != null) {
					rs.close();
				}
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
	}

	public static class JoinKeys {
		public Object[] keys;

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

			return equals(keys, other.keys);
		}

		public static boolean equals(Object[] a, Object[] a2) {
			if (a == a2)
				return true;
			if (a == null || a2 == null)
				return false;

			int length = a.length;
			if (a2.length != length)
				return false;

			for (int i = 0; i < length; i++) {
				Object o1 = a[i];
				Object o2 = a2[i];
				if ((o1 == null || o2 == null) || (!o1.equals(o2)))
					return false;
			}

			return true;
		}
	}

	class SortMergeJoinerCallback implements SortMergeJoinerListener {
		Join join;

		SortMergeJoinerCallback(Join join) {
			this.join = join;
		}

		@Override
		public void onPushPipe(Row row) {
			join.pushPipe(row);
		}
	}
}
