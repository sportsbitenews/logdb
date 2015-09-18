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
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
	private static final int FLUSH_THRESHOLD = 1000000;
	private Integer limit;
	private SortField[] fields;
	private ParallelMergeSorter sorter;
	private TopSelector<Item> top;
	private List<String> partitions;
	private Map<String, List<Item>> sortBuffer;
	private Integer sortBufferLength;

	public Sort(Integer limit, SortField[] fields, List<String> partitions) {
		this.limit = limit;
		this.fields = fields;
		this.partitions = partitions;

		if (partitions.size() > 0) {
			sortBuffer = new HashMap<String, List<Item>>();
			sortBufferLength = 0;
		}
	}

	@Override
	public String getName() {
		return "sort";
	}

	@Override
	public void onStart() {
		if (partitions.size() > 0) {
			this.sorter = new ParallelMergeSorter(new PKSKComparator());
			initParallelMergeSorter();
		} else if (limit != null && limit <= TOP_OPTIMIZE_THRESHOLD) {
			this.top = new TopSelector<Item>(limit, new DefaultComparator());
		} else {
			this.sorter = new ParallelMergeSorter(new DefaultComparator());
			initParallelMergeSorter();
		}
	}

	private void initParallelMergeSorter() {
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
			if (sortBuffer != null) {
				optimizeByClause(m);
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

					if (sortBuffer != null)
						optimizeByClause(row);
					else if (top != null)
						top.add(new Item(row.map(), null));
					else if (sorter != null)
						sorter.add(new Item(row.map(), null));
				}
			} else {
				for (int i = 0; i < rowBatch.size; i++) {
					Row row = rowBatch.rows[i];

					if (sortBuffer != null)
						optimizeByClause(row);
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
					for (List<Item> flushItems : sortBuffer.values()) {
						for (Item flushItem : flushItems) {
							sorter.add(flushItem);
						}
					}
				}

				synchronized (sorter) {
					it = sorter.sort();
				}

				if (sortBuffer != null) {
					String currentPK = "";
					boolean isSkip = false;
					int currentCount = 0;

					while (it.hasNext()) {
						Object o = it.next();
						String partitionKey = (String) ((Item) o).getKey();

						if (!currentPK.equals(partitionKey)) {
							isSkip = false;
							currentCount = 0;
							currentPK = partitionKey;
						} else {
							if (isSkip)
								continue;
						}

						Map<String, Object> value = (Map<String, Object>) ((Item) o).getValue();
						pushPipe(new Row(value));

						if (++currentCount == count)
							isSkip = true;
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

	private void optimizeByClause(Row m) throws IOException {
		if (limit != null && limit <= GROUPBY_LIMIT_THRESHOLD) {
			String partitionKey = getPartitionKey(m);

			synchronized (sortBuffer) {
				List<Item> items = sortBuffer.get(partitionKey);
				if (items == null) {
					items = new ArrayList<Item>(limit);
					items.add(new Item(partitionKey, m.map()));
					sortBuffer.put(partitionKey, items);
				} else {
					if (items.size() == limit) {
						PKSKComparator comparator = new PKSKComparator();
						Item item = Collections.max(items, comparator);
						Item newItem = new Item(partitionKey, m.map());

						if (comparator.compare(newItem, item) < 0)
							items.set(items.indexOf(item), newItem);
					} else {
						items.add(new Item(partitionKey, m.map()));
					}
				}

				if (sortBufferLength >= FLUSH_THRESHOLD) {
					for (List<Item> flushItems : sortBuffer.values()) {
						for (Item flushItem : flushItems) {
							sorter.add(flushItem);
						}
					}

					sortBuffer.clear();
				}
			}
		} else {
			if (top != null) {
				top.add(new Item(m.map(), null));
			} else {
				synchronized (sorter) {
					sorter.add(new Item(m.map(), null));
				}
			}
		}
	}

	private class DefaultComparator implements Comparator<Item> {
		private ObjectComparator cmp = new ObjectComparator();

		@SuppressWarnings("unchecked")
		@Override
		public int compare(Item o1, Item o2) {
			Map<String, Object> m1 = (Map<String, Object>) o1.getKey();
			Map<String, Object> m2 = (Map<String, Object>) o2.getKey();

			return compareMaps(cmp, m1, m2);
		}
	}

	private class PKSKComparator implements Comparator<Item> {
		private ObjectComparator cmp = new ObjectComparator();

		@SuppressWarnings("unchecked")
		@Override
		public int compare(Item o1, Item o2) {
			Map<String, Object> m1 = (Map<String, Object>) o1.getValue();
			Map<String, Object> m2 = (Map<String, Object>) o2.getValue();

			return compareMaps(cmp, m1, m2);
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

	// TODO by clause가 반영된 toString을 찍어내야 함
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

		return "sort" + limitOpt + fieldOpt;
	}

	private String getPartitionKey(Row m) {
		String partitionKey = "";

		for (String partition : partitions) {
			Object o = m.get(partition);
			if (o instanceof String) {
				partitionKey += o;
			} else if (o instanceof Integer) {
				partitionKey += ((Integer) o).toString();
			} else if (o instanceof Date) {
				partitionKey += ((Date) o).toString();
			} else if (o instanceof Boolean) {
				partition += ((Boolean) o) ? "1" : "0";
			}

		}

		return partitionKey;
	}

	private int compareMaps(ObjectComparator cmp, Map<String, Object> m1, Map<String, Object> m2) {
		for (SortField field : fields) {
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
