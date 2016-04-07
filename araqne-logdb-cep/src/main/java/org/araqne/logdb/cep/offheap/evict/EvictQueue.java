package org.araqne.logdb.cep.offheap.evict;

import java.io.Closeable;

import org.araqne.logdb.cep.offheap.storageArea.AbsractUnsafeStorageArea;

public class EvictQueue implements Closeable {
	private int currentSize;
	private EvictStorageArea storage = new EvictStorageArea();

	public EvictQueue() {
		currentSize = 0;
		storage.expansible(true);
	}

	public EvictItem get(int i) {
		return storage.getValue(i);
	}

	public int size() {
		return currentSize;
	}

	public void close() {
		storage.close();
	}

	public void clear() {
		storage.close();
		storage = new EvictStorageArea();
		currentSize = 0;
	}

	public boolean add(EvictItem item) {
		int hole = ++currentSize;
		storage.setValue(0, item);

		for (; compare(item, storage.getValue(hole / 2)) < 0; hole /= 2)
			storage.setValue(hole, storage.getValue(hole / 2));

		storage.setValue(hole, item);
		return true;
	}

	private int compare(EvictItem lhs, EvictItem rhs) {
		if (lhs == null) {
			return -1;
		}
		return lhs.compareTo(rhs);
	}

	/*
	 * this method returns the head of the queue but does not remove that. if
	 * you need a 'poll()' method as Java queue interface, use a 'remove()'
	 * method.
	 */
	public EvictItem peek() {
		if (currentSize < 1)
			return null;

		return storage.getValue(1);
	}

	public EvictItem remove() {
		EvictItem minItem = peek();
		storage.setValue(1, storage.getValue(currentSize--));
		percolateDown(1);

		return minItem;
	}

	private void percolateDown(int hole) {
		int child = hole * 2;
		EvictItem tmp = storage.getValue(hole);

		for (; hole * 2 <= currentSize; hole = child) {
			child = hole * 2;
			if (child != currentSize && compare(storage.getValue(child + 1), storage.getValue(child)) < 0)
				child++;

			if (compare(storage.getValue(child), tmp) < 0)
				storage.setValue(hole, storage.getValue(child));
			else
				break;
		}

		storage.setValue(hole, tmp);
	}

	@SuppressWarnings("restriction")
	private class EvictStorageArea extends AbsractUnsafeStorageArea<EvictItem> {

		public EvictStorageArea() {
			super(16);
		}

		public EvictStorageArea(int initialCapacity) {
			super(16, initialCapacity);
		}

		@Override
		public void setValue(int index, EvictItem value) {
			storage.putLong(index(index), value.getTime());
			storage.putLong(index(index) + 8, value.getAddress());
		}

		@Override
		public EvictItem getValue(int index) {
			long time = storage.getLong(index(index));
			long address = storage.getLong(index(index) + 8);

			return new EvictItem(time, address);
		}
	}

}
