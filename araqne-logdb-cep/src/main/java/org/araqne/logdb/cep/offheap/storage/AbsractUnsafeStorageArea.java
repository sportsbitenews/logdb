package org.araqne.logdb.cep.offheap.storage;

import java.lang.reflect.Field;

import sun.misc.Unsafe;

@SuppressWarnings("restriction")
public abstract class AbsractUnsafeStorageArea<V> implements StorageArea<V> {

	protected Unsafe storage;
	protected long startIndex;
	protected int valueSize;
	protected int capacity;
	private boolean expansible = false;

	private final static int defaultCapacity = Short.MAX_VALUE -1 >> 4;
	/* index가 integer 이므로 max는 integer.max_value*/
	private final static int maxCapacity = Integer.MAX_VALUE - 1;

	public AbsractUnsafeStorageArea(int valueSize) {
		this(valueSize, defaultCapacity);
	}

	public AbsractUnsafeStorageArea(int valueSize, int initialCapacity) {
		if(valueSize <= 0)
			throw new IllegalArgumentException("invalid value size");
		
		if(initialCapacity <= 0)
			throw new IllegalArgumentException("invalid storage size");
		
		this.expansible = false;
		this.storage = getUnsafe();
		this.valueSize = valueSize;
		if((initialCapacity & 0x01) != 0)
			initialCapacity -= 1;
		
		startIndex = allocateMemory(capacity = initialCapacity);
	}

	// public static void main(String[] args) {
	//
	// Unsafe unsafe = getUnsafe();
	// long address = unsafe.allocateMemory(3147483648L);
	// for(int i = 0; i < 500;i++)
	// unsafe.putInt(address +(i * 4), 100 + i);
	// long otherAddress = unsafe.allocateMemory(3000);
	// unsafe.copyMemory(address, otherAddress,2000);
	//
	// for(int i = 0; i < 500;i++) {
	// System.out.println(i + " : " + unsafe.getInt(otherAddress +(i * 4)));
	// }
	// unsafe.freeMemory(address);
	// unsafe.freeMemory(otherAddress);
	// }

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
		if (capacity <= offset)
			inflate();

		return startIndex + (offset) * valueSize;// ;
	}

	// @Override
	// public V getValue(int index) {
	// return null;
	// }

	// @Override
	// public void setValue(int index, V value) {
	// }

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
