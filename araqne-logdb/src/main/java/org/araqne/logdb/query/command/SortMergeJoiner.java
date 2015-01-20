package org.araqne.logdb.query.command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.RowPipe;
import org.araqne.logdb.query.command.Join.JoinKeys;
import org.araqne.logdb.query.command.Join.JoinType;
import org.araqne.logdb.query.command.Sort.SortField;
import org.araqne.logdb.sort.CloseableIterator;
import org.araqne.logdb.sort.Item;
import org.araqne.logdb.sort.MultiRunIterator;
import org.araqne.logdb.sort.ParallelMergeSorter;

public class SortMergeJoiner {
	CloseableIterator rIt;
	CloseableIterator sIt;

	private final JoinType joinType;
	private int joinKeyCount;
	private SortField[] sortFields;

	protected RowPipe output;
	private long outputCount;

	SortMergeJoiner(JoinType joinType, SortField[] sortFields, RowPipe output) {
		this.joinType = joinType;
		this.sortFields = sortFields;
		this.joinKeyCount = sortFields.length;

		this.output = output;
	}

	void setR(RowBatch rowBatch) throws IOException {
		this.rIt = sortR(rowBatch);
	}

	private CloseableIterator sortR(RowBatch rowBatch) throws IOException {
		ParallelMergeSorter sorter = new ParallelMergeSorter(new DefaultComparator());

		Item item = null;
		if (rowBatch.selectedInUse) {
			for (int i = 0; i < rowBatch.size; i++) {
				Row row = rowBatch.rows[rowBatch.selected[i]];
				item = getItem(row);
				sorter.add(item);
			}
		} else {
			for (Row row : rowBatch.rows) {
				item = getItem(row);
				sorter.add(item);
			}
		}

		CloseableIterator ret;
		synchronized (sorter) {
			ret = sorter.sort();
		}

		return ret;
	}

	void setS(Iterator<Map<String, Object>> unsortedS) throws IOException {
		this.sIt = sortS(unsortedS);
	}

	private CloseableIterator sortS(Iterator<Map<String, Object>> it) throws IOException {
		ParallelMergeSorter sorter = new ParallelMergeSorter(new DefaultComparator());

		while (it.hasNext()) {
			Map<String, Object> sm = it.next();

			Object[] keys = new Object[joinKeyCount];
			for (int i = 0; i < joinKeyCount; i++) {
				Object joinValue = sm.get(sortFields[i].getName());
				if (joinValue instanceof Integer || joinValue instanceof Short) {
					joinValue = ((Number) joinValue).longValue();
				}
				keys[i] = joinValue;
			}

			JoinKeys joinKeys = new JoinKeys(keys);

			Item item = new Item(joinKeys.hashCode(), sm);
			sorter.add(item);
		}

		CloseableIterator ret;
		synchronized (sorter) {
			ret = sorter.sort();
		}

		return ret;
	}

	//NOT THREAD SAFE
	void merge() {
		sIt.reset();
		
		Item rItem = null;
		Integer rJoinKey = 0;
		if (rIt.hasNext()) {
			rItem = rIt.next();
			rJoinKey = (Integer) rItem.getKey();
		}
		else
			return;

		Item sItem = null;
		Integer sJoinKey = 0;
		if (sIt.hasNext()) {
			sItem = sIt.next();
			sJoinKey = (Integer) sItem.getKey();
		}
		else
			return;

		while (rItem != null && sItem != null) {
			if (rJoinKey < sJoinKey) {
				if (!rIt.hasNext()) {
					rItem = null;
					rJoinKey = null;
				} else {
					rItem = rIt.next();
					rJoinKey = (Integer) rItem.getKey();
				}
			} else if (rJoinKey > sJoinKey) {
				if (!sIt.hasNext()) {
					sItem = null;
					sJoinKey = null;
				} else {
					sItem = sIt.next();
					sJoinKey = (Integer) sItem.getKey();
				}
			} else {
				int joinKey = rJoinKey;
				ArrayList<Item> sameJoinKeyItems = new ArrayList<Item>();
				while (sItem != null && sJoinKey == joinKey) {
					if (!sIt.hasNext() && sItem != null) {
						sameJoinKeyItems.add(sItem);
						sItem = null;
						sJoinKey = null;
					} else {
						sameJoinKeyItems.add(sItem);
						sItem = sIt.next();
						sJoinKey = (Integer) sItem.getKey();
					}
				}

				while (rItem != null && rJoinKey == joinKey) {
					if (!rIt.hasNext() && rItem != null) {
						pushMergedItem(rItem, sameJoinKeyItems);
						rItem = null;
						rJoinKey = null;
					} else {
						pushMergedItem(rItem, sameJoinKeyItems);
						rItem = rIt.next();
						rJoinKey = (Integer) rItem.getKey();
					}
				}
			}
		}

		/*
		 * int joinKey = rJoinKey; ArrayList<Item> sameJoinKeyItems = new ArrayList<Item>();
		 * while(sIt.hasNext() && sJoinKey == joinKey) { sameJoinKeyItems.add(sItem); sItem = sIt.next();
		 * sJoinKey = (Integer) sItem.getKey(); }
		 * 
		 * while(rItem != null && rJoinKey == joinKey) { //밖으로 합쳐서 뿌려준다.j pushMergedItem(rItem,
		 * sameJoinKeyItems); rItem = null; }
		 */
	}

	private void pushMergedItem(Item rItem, List<Item> sItems) {
		for (Item sItem : sItems) {
			rItem.getValue();
			sItem.getValue();

			Map<String, Object> rLog = new HashMap<String, Object>(((Row) rItem.getValue()).map());
			Map<String, Object> sLog = (Map<String, Object>) sItem.getValue();
			rLog.putAll(sLog);
			pushPipe(new Row(rLog));
		}
	}

	private void pushPipe(Row row) {
		outputCount++;

		if (output != null) {
			if (output.isThreadSafe()) {
				output.onRow(row);
			} else {
				synchronized (output) {
					output.onRow(row);
				}
			}
		}
	}
	
	long getOutputCount() {
		return outputCount;
	}

	private Item getItem(Row row) {
		Object[] keys = new Object[joinKeyCount];
		for (int i = 0; i < joinKeyCount; i++) {
			Object joinValue = row.get(sortFields[i].getName());
			if (joinValue instanceof Integer || joinValue instanceof Short) {
				joinValue = ((Number) joinValue).longValue();
			}
			keys[i] = joinValue;
		}

		JoinKeys joinKeys = new JoinKeys(keys);
		Item item = new Item(joinKeys.hashCode(), row);
		return item;
	}

	private class DefaultComparator implements Comparator<Item> {
		@Override
		public int compare(Item o1, Item o2) {
			int key1 = (Integer) o1.getKey();
			int key2 = (Integer) o2.getKey();
			return key1 - key2;
		}

	}
}
