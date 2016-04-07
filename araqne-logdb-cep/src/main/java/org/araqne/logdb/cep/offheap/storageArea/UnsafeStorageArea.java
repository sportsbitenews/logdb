package org.araqne.logdb.cep.offheap.storageArea;

import java.lang.reflect.Field;

import org.araqne.logdb.cep.offheap.engine.serialize.Serialize;

import sun.misc.Unsafe;

@SuppressWarnings("restriction")
public class UnsafeStorageArea<T> extends AbsractUnsafeStorageArea<T> {

	private Serialize<T> serialize;

	public UnsafeStorageArea(Serialize<T> serialize) {
		super(serialize.size());
		this.serialize = serialize;
	}

	public UnsafeStorageArea(int initialCapacity, Serialize<T> serialize) {
		super(1, initialCapacity);
		this.serialize = serialize;
	}

	@Override
	public T getValue(int index) {
		byte[] ret = new byte[valueSize];

		for (int i = 0; i < valueSize; i++) {
			ret[i] = storage.getByte(index(index) + i);
		}
		return serialize.deserialize(ret);
	}

	@Override
	public void setValue(int index, T v) {
		byte[] value = serialize.serialize(v);

		if (value.length != valueSize)
			throw new IllegalArgumentException("wrong size value");

		for (int i = 0; i < value.length; i++) {
			storage.putByte(index(index) + i, value[i]);
		}
	}

	@Override
	public void remove(int index) {
		byte[] b = new byte[valueSize];
		T def = serialize.deserialize(b);
		setValue(0, def);
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
