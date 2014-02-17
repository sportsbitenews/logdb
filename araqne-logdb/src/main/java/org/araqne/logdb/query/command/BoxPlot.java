/*
 * Copyright 2013 Eediom Inc.
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

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.araqne.logdb.ObjectComparator;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.Row;
import org.araqne.logdb.Strings;
import org.araqne.logdb.query.expr.Expression;
import org.araqne.logdb.sort.CloseableIterator;
import org.araqne.logdb.sort.Item;
import org.araqne.logdb.sort.ParallelMergeSorter;

public class BoxPlot extends QueryCommand {
	private final int clauseCount;

	private ParallelMergeSorter sorter;

	private Expression expr;
	private List<String> clauses;

	// count per group keys
	private Map<GroupKey, AtomicLong> groupCounts;

	public BoxPlot(Expression expr, List<String> clauses) {
		this.expr = expr;
		this.clauses = clauses;
		this.clauseCount = clauses.size();
		this.groupCounts = new HashMap<GroupKey, AtomicLong>();
		this.sorter = new ParallelMergeSorter(new ItemComparer());
	}

	@Override
	public String getName() {
		return "boxplot";
	}

	public Expression getExpression() {
		return expr;
	}

	public List<String> getClauses() {
		return clauses;
	}

	@Override
	public void onPush(Row m) {
		Object value = expr.eval(m);
		if (value == null)
			return;

		Object[] item = new Object[clauseCount + 1];

		int i = 0;
		for (String clause : clauses) {
			Object keyValue = m.get(clause);
			if (keyValue != null)
				item[i] = keyValue;
			i++;
		}

		Object[] keys = Arrays.copyOfRange(item, 0, item.length - 1);
		GroupKey groupKey = new GroupKey(keys);
		AtomicLong count = groupCounts.get(groupKey);
		if (count == null) {
			count = new AtomicLong(1);
			groupCounts.put(groupKey, count);
		} else {
			count.incrementAndGet();
		}

		item[i] = value;

		try {
			sorter.add(new Item(item, null));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void onClose(QueryStopReason reason) {
		long rank = 0;
		long iqr1Index = 0;
		long iqr2Index = 0;
		long iqr3Index = 0;

		Object min = null;
		Object iqr1 = null;
		Object iqr2 = null;
		Object iqr3 = null;
		Object max = null;
		Object last = null;
		Object count = null;
		GroupKey lastGroupKey = null;

		CloseableIterator it;
		try {
			it = sorter.sort();

			while (it.hasNext()) {
				Item item = it.next();
				Object[] values = (Object[]) item.getKey();
				Object value = values[clauseCount];
				if (value == null)
					continue;

				Object[] keys = Arrays.copyOfRange(values, 0, values.length - 1);
				GroupKey groupKey = new GroupKey(keys);

				if (lastGroupKey == null || !lastGroupKey.equals(groupKey)) {
					if (lastGroupKey != null) {
						max = last;
						writeSummary(lastGroupKey, min, iqr1, iqr2, iqr3, max, count);
					}

					long groupSize = groupCounts.get(groupKey).get();
					long quartile = groupSize / 4;
					rank = 0;
					iqr1Index = quartile;
					iqr2Index = quartile * 2;
					iqr3Index = quartile * 3;

					count = groupSize;
					min = value;
					iqr1 = null;
					iqr2 = null;
					iqr3 = null;
					max = null;
				}

				if (rank == iqr1Index)
					iqr1 = value;
				if (rank == iqr2Index)
					iqr2 = value;
				if (rank == iqr3Index)
					iqr3 = value;

				rank++;
				last = value;
				lastGroupKey = groupKey;
			}

			max = last;

			if (lastGroupKey != null)
				writeSummary(lastGroupKey, min, iqr1, iqr2, iqr3, max, count);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void writeSummary(GroupKey groupKey, Object min, Object iqr1, Object iqr2, Object iqr3, Object max, Object count) {
		Map<String, Object> summary = new HashMap<String, Object>();
		int i = 0;
		for (String clause : clauses)
			summary.put(clause, groupKey.keys[i++]);

		summary.put("min", min);
		summary.put("iqr1", iqr1);
		summary.put("iqr2", iqr2);
		summary.put("iqr3", iqr3);
		summary.put("max", max);
		summary.put("count", count);

		pushPipe(new Row(summary));
	}

	@Override
	public boolean isReducer() {
		return true;
	}

	@Override
	public String toString() {
		return "boxplot " + expr + " by " + Strings.join(clauses, ", ");
	}

	private static class ItemComparer implements Comparator<Item> {
		private ObjectComparator cmp = new ObjectComparator();

		@Override
		public int compare(Item o1, Item o2) {
			return cmp.compare(o1.getKey(), o2.getKey());
		}

	}

	private static class GroupKey {
		private Object[] keys;

		public GroupKey(Object[] keys) {
			this.keys = keys;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + Arrays.hashCode(keys);
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			GroupKey other = (GroupKey) obj;
			if (!Arrays.equals(keys, other.keys))
				return false;
			return true;
		}

		@Override
		public String toString() {
			return Arrays.toString(keys);
		}
	}
}
