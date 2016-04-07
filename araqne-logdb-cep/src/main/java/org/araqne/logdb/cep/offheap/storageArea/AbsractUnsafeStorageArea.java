package org.araqne.logdb.cep.offheap.storageArea;

import java.lang.reflect.Field;

import sun.misc.Unsafe;

@SuppressWarnings("restriction")
public abstract class AbsractUnsafeStorageArea<V> implements StorageArea<V> {
	protected Unsafe storage;
	protected long startIndex;
	protected int valueSize;
	protected int capacity;
	private boolean expansible = false;

	private final static int defaultCapacity = Short.MAX_VALUE - 1 >> 4;
	private final static int maxCapacity = Integer.MAX_VALUE - 1;

	public AbsractUnsafeStorageArea(int valueSize) {
		this(valueSize, defaultCapacity);
	}

	public AbsractUnsafeStorageArea(int valueSize, int initialCapacity) {
		if (valueSize <= 0)
			throw new IllegalArgumentException("invalid value size");

		if (initialCapacity <= 0)
			throw new IllegalArgumentException("invalid storage size");

		this.expansible = false;
		this.storage = getUnsafe();
		this.valueSize = valueSize;
		if ((initialCapacity & 0x01) != 0)
			initialCapacity -= 1;

		startIndex = allocateMemory(capacity = initialCapacity);
	}

	private long allocateMemory(long capacity) {
		long start = storage.allocateMemory(capacity * this.valueSize);
		storage.setMemory(start, capacity * valueSize, (byte) 0);
		return start;
	}

	public void expansible(boolean s) {
		expansible = s;
	}

	private void inflate() {
		if (!expansible || capacity >= maxCapacity) {
			throw new IllegalStateException("index out of bounds");
		}

		long newStartIndex = 0;
		int newCapacity = capacity;
		try {
			newCapacity = (capacity * 2 >= maxCapacity || capacity * 2 <= 0) ? maxCapacity : capacity * 2;
			newStartIndex = allocateMemory(newCapacity);
			storage.copyMemory(startIndex, newStartIndex, (long) capacity * valueSize);
			storage.freeMemory(startIndex);
		} catch (Throwable e) {
			throw new IllegalStateException("out of memory");
		}

		startIndex = newStartIndex;
		capacity = newCapacity;
	}

	protected long index(long offset) {
		if (capacity <= offset) {
			if (capacity * 2 <= offset)
				throw new IllegalStateException("index out of bounds");

			inflate();
		}

		return startIndex + (offset) * valueSize;// ;
	}

	@Override
	public void remove(int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() {
		storage.freeMemory(startIndex);
	}

	protected static Unsafe getUnsafe() {
		try {
			Field f = Unsafe.class.getDeclaredField("theUnsafe");
			f.setAccessible(true);
			return (Unsafe) f.get(null);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
}
