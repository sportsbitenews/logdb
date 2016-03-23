package org.araqne.logdb.cep.offheap.storage;

import org.araqne.logdb.cep.offheap.allocator.AllocableArea;
import org.araqne.logdb.cep.offheap.engine.Entry;
import org.araqne.logdb.cep.offheap.engine.serialize.Serialize;

/*
 * entry를 통째로 encoding 하여 기존 byte[]에 저장하였지만 encoding 성능이 느리므로
 * entry의 primitive field는 encoding 하지 않고 바로 저장하자.
 */
@SuppressWarnings("restriction")
public class EntryStorageArea<K, V> extends AbsractUnsafeStorageArea<Entry<K, V>> implements AllocableArea<Entry<K, V>> {
	private Serialize<K> keySerializer;
	private Serialize<V> valueSerializer;

	private static enum Index {
		Hash(0), Next(4), TimeoutTime(12), KeySize(20), ValueSize(24), Key(28);
		private int offset;

		private Index(int i) {
			offset = i;
		}

		public long offset(long index) {
			return index + offset;
		}
	}

	public EntryStorageArea(int initialCapacity, Serialize<K> keySerializer, Serialize<V> valueSerializer) {
		super(1, initialCapacity);
		this.expansible(true);
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
	}

	@Override
	public Entry<K, V> getValue(int index) {
		int hash = storage.getInt(Index.Hash.offset(index(index)));
		long next = storage.getLong(Index.Next.offset(index(index)));
		long timeoutTime = storage.getLong(Index.TimeoutTime.offset(index(index)));
		int keySize = storage.getInt(Index.KeySize.offset(index(index)));
		int valueSize = storage.getInt(Index.ValueSize.offset(index(index)));

		byte[] keys = new byte[keySize];
		for (int i = 0; i < keySize; i++) {
			keys[i] = storage.getByte(Index.Key.offset(index(index)) + i);
		}
		K key = keySerializer.deserialize(keys);

		byte[] values = new byte[valueSize];
		for (int i = 0; i < valueSize; i++) {
			values[i] = storage.getByte(Index.Key.offset(index(index)) + keySize + i);
		}
		V value = valueSerializer.deserialize(values);

		return new Entry<K, V>(key, value, hash, next, timeoutTime);
	}

	@Override
	public void setValue(int index, Entry<K, V> entry) {
		byte[] encodedKey = keySerializer.serialize(entry.getKey());
		byte[] encodedValue = valueSerializer.serialize(entry.getValue());

		storage.putInt(Index.Hash.offset(index(index)), entry.getHash());
		storage.putLong(Index.Next.offset(index(index)), entry.getNext());
		storage.putLong(Index.TimeoutTime.offset(index(index)), entry.getTimeoutTime());
		storage.putInt(Index.KeySize.offset(index(index)), encodedKey.length);
		storage.putInt(Index.ValueSize.offset(index(index)), encodedValue.length);

		// put key
		for (int i = 0; i < encodedKey.length; i++) {
			storage.putByte(Index.Key.offset(index(index)), encodedKey[i]);
		}

		// put value
		for (int i = 0; i < encodedValue.length; i++) {
			storage.putByte(Index.Key.offset(index(index)) + encodedKey.length, encodedValue[i]);
		}
	}

	public K pickKey(int index) {
		int keySize = storage.getInt(Index.KeySize.offset(index(index)));

		byte[] keys = new byte[keySize];
		for (int i = 0; i < keySize; i++) {
			keys[i] = storage.getByte(Index.Key.offset(index(index)) + i);
		}
		return keySerializer.deserialize(keys);
	}

	public V pickValue(int index) {
		int valueSize = storage.getInt(Index.ValueSize.offset(index(index)));
		int keySize = storage.getInt(Index.KeySize.offset(index(index)));

		byte[] values = new byte[valueSize];
		for (int i = 0; i < valueSize; i++) {
			values[i] = storage.getByte(Index.Key.offset(index(index)) + keySize + i);
		}
		return valueSerializer.deserialize(values);
	}

	public long pickNext(int index) {
		return storage.getLong(Index.Next.offset(index(index)));
	}

	public long pickTimeoutTime(int index) {
		return storage.getLong(Index.TimeoutTime.offset(index(index)));
	}

	public int pickHash(int index) {
		return storage.getInt(Index.Hash.offset(index(index)));
	}

	public void setEncodedValue(int index, Entry<K, V> entry, byte[] encodedKey, byte[] encodedValue) {
		storage.putInt(Index.Hash.offset(index(index)), entry.getHash());
		storage.putLong(Index.Next.offset(index(index)), entry.getNext());
		storage.putLong(Index.TimeoutTime.offset(index(index)), entry.getTimeoutTime());
		storage.putInt(Index.KeySize.offset(index(index)), encodedKey.length);
		storage.putInt(Index.ValueSize.offset(index(index)), encodedValue.length);

		// put key
		for (int i = 0; i < encodedKey.length; i++) {
			storage.putByte(Index.Key.offset(index(index)), encodedKey[i]);
		}

		// put value
		for (int i = 0; i < encodedValue.length; i++) {
			storage.putByte(Index.Key.offset(index(index)) + encodedKey.length, encodedValue[i]);
		}
	}

	public void setNext(int index, long next) {
		storage.putLong(Index.Next.offset(index(index)), next);
	}

	public boolean equalsHash(int index, int hash) {
		int oldHash = storage.getInt(Index.Hash.offset(index(index)));
		return hash == oldHash;
	}

	public boolean equalsKey(int index, Object key) {
		int keySize = storage.getInt(Index.KeySize.offset(index(index)));

		byte[] keys = new byte[keySize];
		for (int i = 0; i < keySize; i++) {
			keys[i] = storage.getByte(Index.Key.offset(index(index)) + i);
		}
		K oldKey = keySerializer.deserialize(keys);

		return oldKey.equals(key);
	}

	public boolean replaceValue(int index, byte[] encodedValue, long timeoutTime) {
		storage.putInt(Index.ValueSize.offset(index(index)), encodedValue.length);
		int keySize = storage.getInt(Index.KeySize.offset(index(index)));

		// put value
		for (int i = 0; i < encodedValue.length; i++) {
			storage.putByte(Index.Key.offset(index(index)) + keySize, encodedValue[i]);
		}

		// update timeout time
		long oldTimeoutTime = storage.getLong(Index.TimeoutTime.offset(index(index)));
		if (oldTimeoutTime < timeoutTime) {
			storage.putLong(Index.TimeoutTime.offset(index(index)), timeoutTime);
		}
		return true;
	}

	public static int sizeOf(int keySize, int valueSize) {
		return Index.Key.offset + keySize + valueSize;
	}

	@Override
	public int getAddress(int index) {
		return storage.getInt(index(index));// + (offset * addressSize)));
	}

	@Override
	public int capacity() {
		return capacity;
	}

	@Override
	public void setAddress(int index, int address) {
		storage.putInt(index(index), (int) address);
	}

	public void clear() {
		//TODO free all
	}
	
	// // key는 아는 상태에서 value만 디코딩
	// public V getValue(long address, Object key) {
	// // TODO Auto-generated method stub
	// return null;
	// }
	//
	// public int pickKeySize() {
	// // TODO Auto-generated method stub
	// return 0;
	// }


	// // TODO allocate까지 storage engine에서 함.
	// public long setValue(Entry<K, V> entry) {
	// long index = 0;
	// byte[] encodedKey = keySerializer.serialize(entry.getKey());
	// byte[] encodedValue = valueSerializer.serialize(entry.getValue());
	//
	// storage.putInt(Index.Hash.offset(index(index)), entry.getHash());
	// storage.putLong(Index.Next.offset(index(index)), entry.getNext());
	// storage.putLong(Index.TimeoutTime.offset(index(index)),
	// entry.getTimeoutTime());
	// storage.putInt(Index.KeySize.offset(index(index)), encodedKey.length);
	// storage.putInt(Index.ValueSize.offset(index(index)),
	// encodedValue.length);
	//
	// // put key
	// for (int i = 0; i < encodedKey.length; i++) {
	// storage.putByte(Index.Key.offset(index(index)), encodedKey[i]);
	// }
	//
	// // put value
	// for (int i = 0; i < encodedValue.length; i++) {
	// storage.putByte(Index.Key.offset(index(index)) + encodedKey.length,
	// encodedValue[i]);
	// }
	//
	// return 0;
	// }
	

	// public void setNext(long address, long next) {
	// // TODO Auto-generated method stub
	//
	// }
	//
	// public void remove(long index) {
	// // TODO free
	// }

//	public void close() {
//	}
}
