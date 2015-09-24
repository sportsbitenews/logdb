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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.araqne.logdb.ObjectComparator;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.impl.TopSelector;
import org.araqne.logdb.query.parser.ParseResult;
import org.araqne.logdb.query.parser.QueryTokenizer;
import org.araqne.logdb.sort.CloseableIterator;
import org.araqne.logdb.sort.Item;
import org.araqne.logdb.sort.ParallelMergeSorter;

public class Sort extends QueryCommand {
	private static final int GROUPBY_LIMIT_THRESHOLD = 100;
	private static final int TOP_OPTIMIZE_THRESHOLD = 10000;
	private static final int FLUSH_THRESHOLD = 100000;
	private Integer limit;
	private SortField[] fields;
	private List<String> partitionFields;
	private SortField[] compareFields;
	private ParallelMergeSorter sorter;
	private TopSelector<Item> top;
	private Map<List<Object>, PriorityQueue<Item>> sortBuffer;
	private Integer itemCount;
	private PartitionComparator pComparator;

	public Sort(Integer limit, SortField[] fields, List<String> partitionFields) {
		this.limit = limit;
		this.fields = fields;
		this.partitionFields = partitionFields;

		if (partitionFields.size() > 0) {
			// merge partition fields + sort fields
			List<SortField> l = new ArrayList<SortField>();
			for (String partition : partitionFields) {
				l.add(new SortField(partition));
			}
			for (SortField field : fields) {
				l.add(field);
			}
			compareFields = l.toArray(new SortField[0]);
		} else
			compareFields = fields;
	}

	@Override
	public String getName() {
		return "sort";
	}

	@Override
	public void onStart() {
		if (partitionFields.size() > 0) {
			this.sorter = new ParallelMergeSorter(new PartitionComparator(false));
			initSorter();

			if (limit != null && limit <= GROUPBY_LIMIT_THRESHOLD) {
				sortBuffer = new HashMap<List<Object>, PriorityQueue<Item>>();
				itemCount = 0;
				pComparator = new PartitionComparator(true);
			}
		} else {
			if (limit != null && limit <= TOP_OPTIMIZE_THRESHOLD)
				this.top = new TopSelector<Item>(limit, new DefaultComparator());
			else {
				this.sorter = new ParallelMergeSorter(new DefaultComparator());
				initSorter();
			}
		}
	}

	private void initSorter() {
		int queryId = 0;
		if (getQuery() != null)
			queryId = getQuery().getId();

		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd_HHmmss");
		sorter.setTag("_" + queryId + "_" + df.format(new Date()) + "_");
	}

	public Integer getLimit() {
		return limit;
	}

	public SortField[] getFields() {
		return fields;
	}

	@Override
	public void onPush(Row m) {
		try {
			if (partitionFields.size() > 0) {
				sortbyPartitionFields(m);
			} else if (top != null) {
				top.add(new Item(m.map(), null));
			} else if (sorter != null) {
				// onClose() thread can interfere
				synchronized (sorter) {
					sorter.add(new Item(m.map(), null));
				}
			}
		} catch (IOException e) {
			throw new IllegalStateException("sort failed, query " + query, e);
		}
	}

	@Override
	public void onPush(RowBatch rowBatch) {
		try {
			if (rowBatch.selectedInUse) {
				for (int i = 0; i < rowBatch.size; i++) {
					int p = rowBatch.selected[i];
					Row row = rowBatch.rows[p];

					if (partitionFields.size() > 0)
						sortbyPartitionFields(row);
					else if (top != null)
						top.add(new Item(row.map(), null));
					else if (sorter != null)
						sorter.add(new Item(row.map(), null));
				}
			} else {
				for (int i = 0; i < rowBatch.size; i++) {
					Row row = rowBatch.rows[i];

					if (partitionFields.size() > 0)
						sortbyPartitionFields(row);
					else if (top != null)
						top.add(new Item(row.map(), null));
					else if (sorter != null) {
						synchronized (sorter) {
							sorter.add(new Item(row.map(), null));
						}
					}
				}
			}
		} catch (IOException e) {
			throw new IllegalStateException("sort failed, query " + query, e);
		}
	}

	@Override
	public boolean isReducer() {
		return true;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void onClose(QueryStopReason reason) {
		this.status = Status.Finalizing;
		if (top != null) {
			Iterator<Item> it = top.getTopEntries();
			while (it.hasNext()) {
				Item item = it.next();
				pushPipe(new Row((Map<String, Object>) item.getKey()));
			}

			// support sorter cache GC when query processing is ended
			top = null;

		} else if (sorter != null) {
			// TODO: use LONG instead!
			int count = limit != null ? limit : Integer.MAX_VALUE;

			CloseableIterator it = null;
			try {
				if (reason != QueryStopReason.End && reason != QueryStopReason.PartialFetch) {
					synchronized (sorter) {
						sorter.cancel();
					}
					return;
				}

				if (sortBuffer != null) {
					synchronized (sorter) {
						for (PriorityQueue<Item> flushItems : sortBuffer.values()) {
							for (Item flushItem : flushItems) {
								sorter.add(flushItem);
							}
						}
					}
				}

				synchronized (sorter) {
					it = sorter.sort();
				}

				if (partitionFields.size() > 0) {
					Object[] currentPK = null;
					int currentCount = 0;

					while (it.hasNext()) {
						Object o = it.next();
						Object[] partitionSortKey = (Object[]) ((Item) o).getKey();

						if (currentPK == null || !compareTwoPartitionKeys(currentPK, partitionSortKey)) {
							currentCount = 0;
							currentPK = Arrays.copyOfRange(partitionSortKey, 0, partitionFields.size());
						}

						if (currentCount++ < count) {
							Map<String, Object> value = (Map<String, Object>) ((Item) o).getValue();
							int i = 0;
							for (SortField field : compareFields) {
								value.put(field.getName(), partitionSortKey[i++]);
							}
							pushPipe(new Row(value));
						}
					}
				} else {
					while (it.hasNext()) {
						Object o = it.next();
						if (--count < 0)
							break;

						Map<String, Object> value = (Map<String, Object>) ((Item) o).getKey();
						pushPipe(new Row(value));
					}
				}
			} catch (Throwable t) {
				getQuery().stop(t);
			} finally {
				// close and delete sorted run file
				if (it != null) {
					try {
						it.close();
					} catch (IOException e) {
					}
				}

				// support sorter cache GC when query processing is ended
				sorter = null;
				if (sortBuffer != null)
					sortBuffer = null;
			}
		}
	}

	private synchronized void sortbyPartitionFields(Row m) throws IOException {
		Object[] partitionSortKey = getPartitionSortKey(m);
		Object[] partitionKey = getPartitionKey(m);

		Map<String, Object> vMap = m.map();
		for (SortField field : compareFields) {
			vMap.remove(field.getName());
		}

		if (limit != null && limit <= GROUPBY_LIMIT_THRESHOLD) {
			PriorityQueue<Item> items = sortBuffer.get(Arrays.asList(partitionKey));
			if (items == null) {
				items = new PriorityQueue<Item>(limit, pComparator);
				items.add(new Item(partitionSortKey, vMap));
				sortBuffer.put(Arrays.asList(partitionKey), items);
				itemCount++;
			} else {
				if (items.size() == limit) {
					Item item = items.peek();
					Item newItem = new Item(partitionSortKey, vMap);
					if (pComparator.compare(newItem, item) > 0) {
						items.poll();
						items.add(newItem);
					}
				} else {
					items.add(new Item(partitionSortKey, vMap));
					itemCount++;
				}
			}

			if (itemCount >= FLUSH_THRESHOLD) {
				for (PriorityQueue<Item> flushItems : sortBuffer.values()) {
					for (Item flushItem : flushItems) {
						sorter.add(flushItem);
					}
				}
				sortBuffer.clear();
				itemCount = 0;
			}
		} else {
			Item newItem = new Item(partitionSortKey, vMap);
			sorter.add(newItem);
		}
	}

	private boolean compareTwoPartitionKeys(Object[] currentPK, Object[] partitionSortKey) {
		ObjectComparator cmp = new ObjectComparator();
		for (int i = 0; i < partitionFields.size(); ++i) {
			Object v1 = currentPK[i];
			Object v2 = partitionSortKey[i];

			int diff = cmp.compare(v1, v2);
			if (diff != 0)
				return false;
		}

		return true;
	}

	private class DefaultComparator implements Comparator<Item> {
		private ObjectComparator cmp = new ObjectComparator();

		@SuppressWarnings("unchecked")
		@Override
		public int compare(Item o1, Item o2) {
			Map<String, Object> m1 = (Map<String, Object>) o1.getKey();
			Map<String, Object> m2 = (Map<String, Object>) o2.getKey();

			for (SortField field : compareFields) {
				Object v1 = m1.get(field.name);
				Object v2 = m2.get(field.name);

				boolean lhsNull = v1 == null;
				boolean rhsNull = v2 == null;

				if (lhsNull && rhsNull)
					continue;
				else if (lhsNull)
					return field.asc ? -1 : 1;
				else if (rhsNull)
					return field.asc ? 1 : -1;

				int diff = cmp.compare(v1, v2);
				if (diff != 0) {
					if (!field.asc)
						diff *= -1;

					return diff;
				}
			}

			return 0;
		}
	}

	private class PartitionComparator implements Comparator<Item> {
		private ObjectComparator cmp = new ObjectComparator();
		private boolean reverse = false;

		public PartitionComparator(boolean reverse) {
			this.reverse = reverse;
		}

		@Override
		public int compare(Item o1, Item o2) {
			Object[] m1 = (Object[]) o1.getKey();
			Object[] m2 = (Object[]) o2.getKey();

			int i = 0;
			for (SortField field : compareFields) {
				Object v1 = m1[i];
				Object v2 = m2[i];

				boolean lhsNull = v1 == null;
				boolean rhsNull = v2 == null;

				if (lhsNull && rhsNull)
					continue;
				else if (lhsNull)
					return field.asc ? -1 : 1;
				else if (rhsNull)
					return field.asc ? 1 : -1;

				int diff = cmp.compare(v1, v2);
				if (diff != 0) {
					if (!field.asc)
						diff *= -1;

					return (reverse) ? diff *= -1 : diff;
				}

				i++;
			}

			return 0;
		}
	}

	public static class SortField {
		private String name;
		private boolean asc;

		public static List<SortField> parseSortFields(String line, ParseResult r) {
			List<SortField> fields = new ArrayList<SortField>();
			int next = r.next;
			while (true) {
				r = QueryTokenizer.nextString(line, next, ',');
				String token = (String) r.value;
				boolean asc = true;
				char sign = token.charAt(0);
				if (sign == '-') {
					token = token.substring(1);
					asc = false;
				} else if (sign == '+') {
					token = token.substring(1);
				}

				SortField field = new SortField(token.trim(), asc);
				fields.add(field);
				next = r.next;

				if (line.length() == r.next)
					break;
			}

			return fields;
		}

		public static String serialize(SortField[] sortFields) {
			StringBuilder sb = new StringBuilder();
			int i = 0;
			for (SortField f : sortFields) {
				if (i++ != 0)
					sb.append(", ");

				if (!f.isAsc())
					sb.append("-");

				sb.append(f.getName());
			}

			return sb.toString();
		}

		public SortField(String name) {
			this(name, true);
		}

		public SortField(String name, boolean asc) {
			this.name = name;
			this.asc = asc;
		}

		public String getName() {
			return name;
		}

		public boolean isAsc() {
			return asc;
		}

		public void reverseAsc() {
			asc = !asc;
		}

		@Override
		public String toString() {
			return "SortField [name=" + name + ", asc=" + asc + "]";
		}
	}

	@Override
	public String toString() {
		String limitOpt = "";
		if (limit != null)
			limitOpt = " limit=" + limit;

		int i = 0;
		String fieldOpt = "";
		for (SortField f : fields) {
			if (i++ != 0)
				fieldOpt += ",";
			fieldOpt += " " + (f.isAsc() ? "" : "-") + f.getName();
		}

		String partitionOpt = "";
		for (String partition : partitionFields) {
			if (i++ != 0)
				partitionOpt += ",";
			else
				partitionOpt += "by";
			partitionOpt += " " + partition;
		}

		return "sort" + limitOpt + fieldOpt + partitionOpt;
	}

	private Object[] getPartitionKey(Row m) {
		Object[] partitionKey = new Object[partitionFields.size()];

		for (int i = 0; i < partitionKey.length; ++i) {
			partitionKey[i] = m.get(partitionFields.get(i));
		}

		return partitionKey;
	}

	private Object[] getPartitionSortKey(Row m) {
		Object[] partitionSortKey = new Object[compareFields.length];

		for (int i = 0; i < compareFields.length; ++i) {
			partitionSortKey[i] = m.get(compareFields[i].getName());
		}

		return partitionSortKey;
	}
}
