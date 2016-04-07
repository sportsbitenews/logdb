package org.araqne.logdb.cep.offheap.manager;

import java.lang.reflect.Field;

import org.araqne.logdb.cep.offheap.engine.Entry;
import org.araqne.logdb.cep.offheap.engine.serialize.Serialize;

import sun.misc.Unsafe;

@SuppressWarnings("restriction")
public class EntryStorageManagerImpl<K, V> implements EntryStorageManager<K, V> {
	private Serialize<K> keySerializer;
	private Serialize<V> valueSerializer;
	private final int minChunkSize;
	private Unsafe unsafe;

	private enum Offset {
		Hash(0), Next(4), EvictTime(12), MaxSize(20), KeySize(24), ValueSize(28), Key(32);
		private int offset;

		private Offset(int i) {
			offset = i;
		}

		public long get(long index) {
			return index + offset;
		}
	}

	public EntryStorageManagerImpl(int minChunkSize, Serialize<K> keySerializer, Serialize<V> valueSerializer) {
		this.unsafe = getUnsafe();
		this.minChunkSize = minChunkSize;
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
	}

	// allocate and put new entry
	public long set(Entry<K, V> entry) {
		// encode
		byte[] encodedKey = keySerializer.serialize(entry.getKey());
		byte[] encodedValue = valueSerializer.serialize(entry.getValue());

		// allocate
		int requireSize = align(sizeOf(encodedKey.length, encodedValue.length));
		long address = unsafe.allocateMemory(requireSize);
		entry.setMaxSize(requireSize);

		// set value
		setHash(address, entry.getHash());
		setNext(address, entry.getNext());
		setEvictTime(address, entry.getEvictTime());
		setKeySize(address, encodedKey.length);
		setValueSize(address, encodedValue.length);
		setMaxSize(address, entry.getMaxSize());
		setKeyByte(address, encodedKey);
		setValueByte(address, encodedValue);

		return address;
	}

	private int sizeOf(int keySize, int valueSize) {
		return Offset.Key.offset + keySize + valueSize;
	}

	private int align(int i) {
		if (i < minChunkSize)
			return minChunkSize;

		return minChunkSize * ((i / minChunkSize) + 1);
	}

	public void setHash(long address, int hash) {
		unsafe.putInt(Offset.Hash.get(address), hash);
	}

	public void setNext(long address, long next) {
		unsafe.putLong(Offset.Next.get(address), next);
	}

	public void setEvictTime(long address, long timeoutTime) {
		unsafe.putLong(Offset.EvictTime.get(address), timeoutTime);
	}

	public void setMaxSize(long address, int maxSize) {
		unsafe.putInt(Offset.MaxSize.get(address), maxSize);
	}

	public void setKeySize(long address, int keySize) {
		unsafe.putInt(Offset.KeySize.get(address), keySize);
	}

	private void setKeyByte(long address, byte[] keyByte) {
		for (int i = 0; i < keyByte.length; i++) {
			unsafe.putByte(Offset.Key.get(address) + i, keyByte[i]);
		}
	}

	public void setKey(long address, K key) {
		setKeyByte(address, keySerializer.serialize(key));
	}

	public void setValueSize(long address, int valueSize) {
		unsafe.putInt(Offset.ValueSize.get(address), valueSize);
	}

	private void setValueByte(long address, byte[] encodedValue) {
		setValueByte(address, encodedValue, getKeySize(address));
	}

	private void setValueByte(long address, byte[] encodedValue, int keySize) {
		for (int i = 0; i < encodedValue.length; i++) {
			unsafe.putByte(Offset.Key.get(address) + keySize + i, encodedValue[i]);
		}
	}

	public void setValue(long address, V value) {
		setValueByte(address, valueSerializer.serialize(value));
	}

	public Entry<K, V> get(long address) {
		return getEntry(address);
	}

	public int getHash(long address) {
		return unsafe.getInt(Offset.Hash.get(address));
	}

	public long getNext(long address) {
		return unsafe.getLong(Offset.Next.get(address));
	}

	public long getEvictTime(long address) {
		return unsafe.getLong(Offset.EvictTime.get(address));
	}

	public int getKeySize(long address) {
		return unsafe.getInt(Offset.KeySize.get(address));
	}

	public int getValueSize(long address) {
		return unsafe.getInt(Offset.ValueSize.get(address));
	}

	public int getMaxSize(long address) {
		return unsafe.getInt(Offset.MaxSize.get(address));
	}

	public K getKey(long address) {
		return keySerializer.deserialize(getKeyByte(address));
	}

	private byte[] getKeyByte(long address) {
		int keySize = unsafe.getInt(Offset.KeySize.get(address));

		byte[] keys = new byte[keySize];
		for (int i = 0; i < keySize; i++) {
			keys[i] = unsafe.getByte(Offset.Key.get(address) + i);
		}
		return keys;
	}

	public V getValue(long address) {
		return valueSerializer.deserialize(getValueByte(address));
	}

	private byte[] getValueByte(long address) {
		int valueSize = unsafe.getInt(Offset.ValueSize.get(address));
		int keySize = unsafe.getInt(Offset.KeySize.get(address));

		byte[] values = new byte[valueSize];
		for (int i = 0; i < valueSize; i++) {
			values[i] = unsafe.getByte(Offset.Key.get(address) + keySize + i);
		}
		return values;
	}

	private Entry<K, V> getEntry(long address) {
		K key = getKey(address);
		V value = getValue(address);
		int hash = getHash(address);
		long next = getNext(address);
		long timeoutTime = getEvictTime(address);
		int maxSize = getMaxSize(address);

		return new Entry<K, V>(key, value, hash, next, timeoutTime, maxSize);
	}

	public long updateValue(long address, V value, long timeoutTime) {
		byte[] encodedValue = valueSerializer.serialize(value);
		int maxSize = getMaxSize(address);
		int keySize = getKeySize(address);
		int size = sizeOf(getKeySize(address), encodedValue.length);
		long oldTimeoutTime = getEvictTime(address);

		if (size < maxSize) {
			setValueByte(address, encodedValue, keySize);
			setValueSize(address, encodedValue.length);
		} else {
			long oldAddress = address;
			int requireSize = align(size);
			address = unsafe.allocateMemory(requireSize);
			unsafe.copyMemory(oldAddress, address, Offset.Key.offset + keySize);

			setValueSize(address, encodedValue.length);
			setMaxSize(address, requireSize);
			setValueByte(address, encodedValue);
			unsafe.freeMemory(oldAddress);
		}

		if (timeoutTime > oldTimeoutTime)
			setEvictTime(address, timeoutTime);

		return address;
	}

	public long replaceValue(long address, V oldValue, V newValue, long timeoutTime) {
		if (!equalsValue(address, oldValue)) {
			return 0L;
		}

		return updateValue(address, newValue, timeoutTime);
	}

	public boolean equalsHash(long address, int hash) {
		int oldHash = getHash(address);
		return hash == oldHash;
	}

	public boolean equalsKey(long address, K key) {
		byte[] encodedKey = keySerializer.serialize(key);
		int keySize = getKeySize(address);

		for (int i = 0; i < keySize; i++) {
			if (encodedKey[i] != unsafe.getByte(Offset.Key.get(address) + i))
				return false;
		}
		return true;
	}

	public boolean equalsValue(long address, V value) {
		V oldValue = getValue(address);
		return value.equals(oldValue);
	}

	/*
	 * public boolean equalsKey(long address, K key) { K oldKey =
	 * getKey(address); return oldKey.equals(key); }
	 */

	public void remove(long address) {
		unsafe.freeMemory(address);
	}

	private static Unsafe getUnsafe() {
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
