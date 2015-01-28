package org.araqne.logdb.query.command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.ObjectComparator;
import org.araqne.logdb.Row;
import org.araqne.logdb.query.command.Join.JoinType;
import org.araqne.logdb.query.command.Sort.SortField;
import org.araqne.logdb.sort.CloseableIterator;
import org.araqne.logdb.sort.Item;
import org.araqne.logdb.sort.ParallelMergeSorter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SortMergeJoiner {
	private final Logger logger = LoggerFactory.getLogger(SortMergeJoiner.class);

	private final JoinType joinType;
	private SortField[] sortFields;

	private ParallelMergeSorter rSorter;
	private ParallelMergeSorter sSorter;
	private DefaultComparator comparator;

	boolean canceled;

	SortMergeJoinerListener listener;

	SortMergeJoiner(JoinType joinType, SortField[] sortFields, SortMergeJoinerListener listener) {
		this.joinType = joinType;
		this.sortFields = sortFields;

		this.comparator = new DefaultComparator();
		this.rSorter = new ParallelMergeSorter(comparator);
		this.sSorter = new ParallelMergeSorter(comparator);

		this.canceled = false;

		this.listener = listener;
	}

	void setR(Row row) throws IOException {
		Item item = getItem(row.map());
		synchronized (rSorter) {
			rSorter.add(item);
		}
	}

	void setS(Iterator<Map<String, Object>> it) throws IOException {
		while (it.hasNext()) {
			Map<String, Object> sm = it.next();
			Item item = getItem(sm);
			synchronized (sSorter) {
				sSorter.add(item);
			}
		}
	}

	void merge() {
		CloseableIterator rIt = null;
		CloseableIterator sIt = null;

		try {
			rIt = rSorter.sort();
			sIt = sSorter.sort();

			if (joinType == JoinType.Inner) {
				innerJoinMerge(rIt, sIt);
			} else if (joinType == JoinType.Left) {
				leftJoinMerge(rIt, sIt);
			} else {
				throw new UnsupportedOperationException("Unsupported Join Type " + joinType.toString());
			}
		} catch (Throwable t) {
			logger.error("araqne logdb: cannot run ParallelMergeSorter at SortMergeJoiner's merge", t);
		} finally {
			try {
				if (rIt != null)
					rIt.close();

				if (sIt != null)
					sIt.close();
			} catch (Throwable t) {
				logger.error("araqne logdb: cannot close CloseableIterator at SortMergeJoiner's merge", t);
			}
		}
	}

	void cancel() throws IOException {
		this.canceled = true;

		rSorter.cancel();
		sSorter.cancel();
	}

	private void innerJoinMerge(CloseableIterator rIt, CloseableIterator sIt) {
		Item rItem = null;
		if (rIt.hasNext()) {
			rItem = rIt.next();
		}
		else
			return;

		Item sItem = null;
		if (sIt.hasNext()) {
			sItem = sIt.next();
		}
		else
			return;

		while (this.canceled == false && rItem != null && sItem != null) {
			int compareResult = comparator.compare(rItem, sItem);
			if (compareResult < 0) {
				if (!rIt.hasNext()) {
					rItem = null;
				} else {
					rItem = rIt.next();
				}
			} else if (compareResult > 0) {
				if (!sIt.hasNext()) {
					sItem = null;
				} else {
					sItem = sIt.next();
				}
			} else {
				Item joinItem = rItem;

				ArrayList<Item> sameJoinKeyItems = new ArrayList<Item>();
				while (sItem != null && hasSameJoinKey(joinItem, sItem)) {
					if (!sIt.hasNext() && sItem != null) {
						sameJoinKeyItems.add(sItem);
						sItem = null;
					} else {
						sameJoinKeyItems.add(sItem);
						sItem = sIt.next();
					}
				}

				while (rItem != null && hasSameJoinKey(joinItem, rItem)) {
					if (!rIt.hasNext() && rItem != null) {
						pushMergedItem(rItem, sameJoinKeyItems);
						rItem = null;
					} else {
						pushMergedItem(rItem, sameJoinKeyItems);
						rItem = rIt.next();
					}
				}
			}
		}
	}

	private void leftJoinMerge(CloseableIterator rIt, CloseableIterator sIt) {
		Item rItem = null;
		if (rIt.hasNext()) {
			rItem = rIt.next();
		}

		Item sItem = null;
		if (sIt.hasNext()) {
			sItem = sIt.next();
		}

		while (this.canceled == false && rItem != null && sItem != null) {
			int compareReulst = comparator.compare(rItem, sItem);
			if (compareReulst < 0) {
				pushMergedItem(rItem);

				if (!rIt.hasNext()) {
					rItem = null;
				} else {
					rItem = rIt.next();
				}
			} else if (compareReulst > 0) {
				if (!sIt.hasNext()) {
					sItem = null;
				} else {
					sItem = sIt.next();
				}
			} else {
				Item joinItem = rItem;

				ArrayList<Item> sameJoinKeyItems = new ArrayList<Item>();
				while (sItem != null && hasSameJoinKey(joinItem, sItem)) {
					if (!sIt.hasNext() && sItem != null) {
						sameJoinKeyItems.add(sItem);
						sItem = null;
					} else {
						sameJoinKeyItems.add(sItem);
						sItem = sIt.next();
					}
				}

				while (rItem != null && hasSameJoinKey(joinItem, rItem)) {
					if (!rIt.hasNext() && rItem != null) {
						pushMergedItem(rItem, sameJoinKeyItems);
						rItem = null;
					} else {
						pushMergedItem(rItem, sameJoinKeyItems);
						rItem = rIt.next();
					}
				}
			}
		}

		while (this.canceled == false && rItem != null) {
			pushMergedItem(rItem);

			if (!rIt.hasNext()) {
				rItem = null;
			} else {
				rItem = rIt.next();
			}
		}
	}

	private boolean hasSameJoinKey(Item item1, Item item2) {
		return comparator.compare(item1, item2) == 0;
	}

	@SuppressWarnings("unchecked")
	private void pushMergedItem(Item rItem, List<Item> sItems) {
		for (Item sItem : sItems) {
			Map<String, Object> rLog = new HashMap<String, Object>((Map<String, Object>) rItem.getKey());
			Map<String, Object> sLog = (Map<String, Object>) sItem.getKey();

			rLog.putAll(sLog);
			listener.onPushPipe(new Row(rLog));
		}
	}

	@SuppressWarnings("unchecked")
	private void pushMergedItem(Item rItem) {
		Map<String, Object> rLog = new HashMap<String, Object>((Map<String, Object>) rItem.getKey());
		listener.onPushPipe(new Row(rLog));
	}

	private Item getItem(Map<String, Object> log) {
		Item item = new Item(log, null);
		return item;
	}

	private class DefaultComparator implements Comparator<Item> {
		private ObjectComparator cmp = new ObjectComparator();

		@SuppressWarnings("unchecked")
		@Override
		public int compare(Item o1, Item o2) {
			Map<String, Object> m1 = (Map<String, Object>) o1.getKey();
			Map<String, Object> m2 = (Map<String, Object>) o2.getKey();

			for (SortField field : sortFields) {
				Object v1 = m1.get(field.getName());
				Object v2 = m2.get(field.getName());

				boolean lhsNull = v1 == null;
				boolean rhsNull = v2 == null;

				if (lhsNull && rhsNull)
					continue;
				else if (lhsNull)
					return field.isAsc() ? -1 : 1;
				else if (rhsNull)
					return field.isAsc() ? 1 : -1;

				int diff = cmp.compare(v1, v2);
				if (diff != 0) {
					if (!field.isAsc())
						diff *= -1;

					return diff;
				}
			}

			return 0;
		}
	}

	interface SortMergeJoinerListener {
		public void onPushPipe(Row row);
	}

}
