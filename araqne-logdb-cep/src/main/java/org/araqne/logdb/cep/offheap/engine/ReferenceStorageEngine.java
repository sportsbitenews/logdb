package org.araqne.logdb.cep.offheap.engine;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.araqne.logdb.cep.offheap.allocator.AllocableArea;
import org.araqne.logdb.cep.offheap.allocator.Allocator;
import org.araqne.logdb.cep.offheap.allocator.IntegerFirstFitAllocator;
import org.araqne.logdb.cep.offheap.engine.serialize.Serialize;
import org.araqne.logdb.cep.offheap.storage.UnsafeByteArrayStorageArea;
import org.araqne.logdb.cep.offheap.storage.UnsafeLongStorageArea;
import org.araqne.logdb.cep.offheap.timeout.OffHeapEventListener;
import org.araqne.logdb.cep.offheap.timeout.TimeoutItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReferenceStorageEngine<K, V> implements StorageEngine<K, V> {
	private final Logger slog = LoggerFactory.getLogger(ReferenceStorageEngine.class);
	private final static long NULL = 0;
	private final static int defaultIndexSize = 1024;// 1024 * 1024 * 16; // 2 ^
														// 24
	private final static int defaultStorageSize = 2047;

	private UnsafeLongStorageArea indexTable;
	private Serialize<K> keySerializer;
	private Serialize<V> valueSerializer;
	private List<AllocableArea<byte[]>> pageTable = new ArrayList<AllocableArea<byte[]>>();
	private List<Allocator> allocators = new ArrayList<Allocator>();
	private int tableSize;
	private int storageSize;
	private List<OffHeapEventListener<K, V>> listeners = new ArrayList<OffHeapEventListener<K, V>>();

	public ReferenceStorageEngine(Serialize<K> keySerializer, Serialize<V> valueSerializer) {
		this(defaultIndexSize, defaultStorageSize, keySerializer, valueSerializer);
	}

	public ReferenceStorageEngine(int indexSize, Serialize<K> keySerializer, Serialize<V> valueSerializer) {
		this(indexSize, defaultStorageSize, keySerializer, valueSerializer);
	}

	public ReferenceStorageEngine(int indexSize, int storageSize, Serialize<K> keySerializer,
			Serialize<V> valueSerializer) {
		this.tableSize = indexSize;
		this.storageSize = storageSize;
		this.indexTable = new UnsafeLongStorageArea(indexSize);
		this.indexTable.expansible(true);
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		// addPage();
	}

	private void addPage() {
		AllocableArea<byte[]> storage = new UnsafeByteArrayStorageArea(storageSize);
		pageTable.add(storage);
		allocators.add(new IntegerFirstFitAllocator(storage));
	}

	/**
	 * hash 값에 해당하는 여러 값 중 가장 앞에 있는 entry를 가져옴 (table의 i에 있는 첫번째 entry를 가져옴)
	 */
	@Override
	public Entry<K, V> get(int hash) {
		return read(indexTable.getValue(indexFor(hash)));
	}

	/**
	 * entry의 다음 (linked 또는 array) entry를 가져온다.-> 이 엔진은 linked list 구조로 구현)
	 */
	@Override
	public Entry<K, V> next(Entry<K, V> e) {
		return read(e.getNext());
	}

	private Entry<K, V> read(long address) {
		if (address == NULL)
			return null;

		AllocableArea<byte[]> storage = pageTable.get(pageNo(address));
		byte[] value = storage.getValue((int) address);
		return Entry.decode(value, keySerializer, valueSerializer, address);
	}

	/*
	 * 기존 값이 존재하는 공간이 신규값보다 크면 그 위치에 저장 아니면 기존 값 삭제후 다시 add
	 */
	@Override
	public long update(Entry<K, V> entry, Entry<K, V> prev, V value) {
		// int index = indexFor(hash);
		// long next = indexTable.getValue(index);
		// Entry<K, V> entry = new Entry<K, V>(key, value, hash, next,
		// timeoutTime, 0L);
		//
		// Object[] b = Entry.marshal(entry, keySerializer, valueSerializer);
		// long address = allocate(Entry.lengthOf(b) + 4);
		// AllocableArea<byte[]> storage = pageTable.get(pageNo(address));
		// storage.setValue((int) address, Entry.encodeEntry(b));
		// indexTable.setValue(index, address);
		// return address;

		long oldAddress = entry.getAddress();
		entry.setValue(value);
		Object[] marshaled = Entry.marshal(entry, keySerializer, valueSerializer);

		int lengthOf = Entry.lengthOf(marshaled) + 4;
		Allocator allocator = allocators.get(pageNo(oldAddress));
		// System.out.println(oldAddress + " " +
		// allocator.space((int)oldAddress));
		if (allocator.space((int) oldAddress) >= lengthOf) {
			// 그위치에 저장
			AllocableArea<byte[]> storage = pageTable.get(pageNo(oldAddress));
			storage.setValue((int) oldAddress, Entry.encode(marshaled, lengthOf - 4));
			return oldAddress;
		}
		remove(entry, prev);

		int index = indexFor(entry.getHash());
		long address = allocate(lengthOf);
		AllocableArea<byte[]> storage = pageTable.get(pageNo(address));
		storage.setValue((int) address, Entry.encode(marshaled, lengthOf - 4));
		indexTable.setValue(index, address);
		return address;

		// V oldValue = target.getValue();
		// target.setValue(value);
		// Entry.lengthOf(entry, keySerializer, valueSerializer)
		// if (valueSerializer.serialize(oldValue).length ==
		// valueSerializer.serialize(value).length) {
		// // old value와 new value 크기가 같으면 그 값만 변경
		// AllocableArea<byte[]> storage =
		// pageTable.get(pageNo(target.getAddress()));
		// storage.setValue((int) target.getAddress(), Entry.encodeEntry(target,
		// keySerializer, valueSerializer));
		// return target.getAddress();
		// } else {
		// // 기존 entry는 지우고 새로운 entry 추가
		// target.setValue(value);
		// return add(target.getHash(), target.getKey(), value,
		// target.getTimeoutTime());
	}

	/*
	 * entry list에서 해당 entry를 삭제한다.
	 */
	@Override
	public Entry<K, V> remove(Entry<K, V> target, Entry<K, V> prev) {
		Allocator allocator = allocators.get(pageNo(target.getAddress()));
		// System.out.println(allocator);
		if (prev == null) { /* head일때 */
			indexTable.setValue(indexFor(target.getHash()), target.getNext());
		} else {
			if (prev.getNext() != target.getAddress())
				// entries does not match
				throw new IllegalStateException();

			/* prev의 next address update (next만 변경했으므로 기존 크기와 일치 ->같은 주소에 덮어쓰기) */
			prev.setNext(target.getNext());
			AllocableArea<byte[]> storage = pageTable.get(pageNo(prev.getAddress()));
			storage.setValue((int) prev.getAddress(), Entry.encode(prev, keySerializer, valueSerializer));
		}

		AllocableArea<byte[]> storage = pageTable.get(pageNo(target.getAddress()));
		storage.remove((int) target.getAddress());

		// System.out.println(pageNo(target.getAddress()));
		// System.out.println(allocator);
		allocator.free((int) target.getAddress());

		// cnt.decrementAndGet();
		// TODO Shrink
		return prev;
	}

	/**
	 * index에 키, 밸류를 추가함 (linked list의 제일 앞). 현재 index에 저장된 값을 신규 node의 next에 추가
	 */
	@Override
	public long add(int hash, K key, V value) {
		return add(hash, key, value, 0L);
	}

	@Override
	public long add(int hash, K key, V value, long timeoutTime) {
		int index = indexFor(hash);
		long next = indexTable.getValue(index);
		Entry<K, V> entry = new Entry<K, V>(key, value, hash, next, timeoutTime, 0L);

		Object[] marshaled = Entry.marshal(entry, keySerializer, valueSerializer);
		int lengthof = Entry.lengthOf(marshaled);
		long address = allocate(lengthof + 4);
		AllocableArea<byte[]> storage = pageTable.get(pageNo(address));
		storage.setValue((int) address, Entry.encode(marshaled, lengthof));
		indexTable.setValue(index, address);

		return address;
	}

	// entry의 timeout time 과 item의 timeout time이 일치하면 삭제
	@Override
	public void evict(TimeoutItem item) {
		AllocableArea<byte[]> storage = pageTable.get(pageNo(item.getAddress()));
		byte[] value = storage.getValue((int) item.getAddress());
		if (value == null || value.length == 0)
			return;

		Entry<K, V> entry = Entry.decode(value, keySerializer, valueSerializer, item.getAddress());
		int hash = entry.getHash();
		Entry<K, V> prev = null;
		for (Entry<K, V> e = get(hash); e != null; e = next(e)) {
			if (e != null && e.equalsKey(entry.getKey()) && e.getTimeoutTime() == item.getTime()) {
				remove(e, prev);

				for (OffHeapEventListener<K, V> listener : listeners) {
					listener.onExpire(e.getKey(), e.getValue(), item.getTime());
				}
			}
			prev = e;
		}
	}

	@Override
	public long updateTime(Entry<K, V> oldValue, Entry<K, V> prev, long timeoutTime) {
		// V oldValue = target.getValue();

		// XXX // old value와 new value 크기가 같으면 그 값만 변경
		// target.setValue(value);
		// if (Entry.lengthOf(Entry.marshal(target, keySerializer,
		// valueSerializer)) == valueSerializer.serialize(value).length) {
		// AllocableArea<byte[]> storage =
		// pageTable.get(pageNo(target.getAddress()));
		// storage.setValue((int) target.getAddress(), Entry.encodeEntry(target,
		// keySerializer, valueSerializer));
		// return target.getAddress();
		// } else {
		// 기존 entry는 지우고 새로운 entry 추가

		remove(oldValue, prev);
		return add(oldValue.getHash(), oldValue.getKey(), oldValue.getValue(), timeoutTime);
	}

	//
	// Allocator allocator = allocators.get(0);
	// System.out.println(allocator);
	//
	// long address = entry.getAddress();
	// // entry.setTimeoutTime(timeoutTime);
	// AllocableArea<byte[]> storage = pageTable.get(pageNo(address));
	// storage.setValue((int) address, Entry.encodeEntry(entry, keySerializer,
	// valueSerializer));
	//
	//
	// System.out.println(allocator);
	//

	// System.out.println("set value " + address);
	// indexTable.setValue(index, address);

	// System.out.println(allocator);
	// System.out.println("length of " + Entry.encodeEntry(entry, keySerializer,
	// valueSerializer).length);

	// if( cnt.getAndIncrement() > tableSize * loadFactor)
	// inflateIndexTable();
	// return address;

	// TODO Auto-generated method stub
	// }

	public void addListener(OffHeapEventListener<K, V> listener) {
		listeners.add(listener);
	}

	public void removeListner(OffHeapEventListener<K, V> listener) {
		listeners.remove(listener);
	}

	// private void inflateIndexTable() {
	// }

	@Override
	public void clear() {
		safeClose(indexTable);
		indexTable = new UnsafeLongStorageArea(tableSize);

		for (Allocator allocator : allocators) {
			allocator.clear();
		}

		// cnt = new AtomicInteger(0);
	}

	@Override
	public Iterator<K> getKeys() {
		return new KeyIterator();
	}

	// public Set<K> keySet() {
	// Set<K> keySet = new HashSet<K>();
	//
	// int index = 0;
	// long address = 0;
	//
	// while (index < tableSize) {
	// if ((address = indexTable.getValue(index++)) == NULL)
	// continue;
	//
	// for (Entry<K, V> e = read(address); e != null; e = next(e)) {
	// keySet.add(e.getKey());
	// }
	// }
	//
	// return keySet;
	// }

	private class KeyIterator implements Iterator<K> {
		int index = 0;
		Entry<K, V> entry = null;

		@Override
		public boolean hasNext() {
			if (entry != null && entry.getNext() != NULL) {
				return true;
			}

			// 대부분은 entry.next가 null일
			int tempIndex = index;
			while (tempIndex < tableSize) {
				long address = indexTable.getValue(tempIndex++);
				if (address == NULL)
					continue;

				index = tempIndex - 1;
				return true;
			}
			return false;
		}

		private Entry<K, V> nextEntry(Entry<K, V> entry) {
			if (entry != null && entry.getNext() != NULL) {
				return read(entry.getNext());
			}

			while (index < tableSize) {
				long address = indexTable.getValue(index++);
				if (address == NULL)
					continue;

				return read(address);
			}
			return null;
		}

		@Override
		public K next() {
			entry = nextEntry(entry);
			if (entry != null)
				return entry.getKey();
			else
				return null;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

	}

	@Override
	public void close() {
		safeClose(indexTable);
		indexTable = null;

		for (AllocableArea<byte[]> page : pageTable) {
			safeClose(page);
		}

		pageTable.clear();
		allocators.clear();
	}

	@Override
	public Entry<K, V> load(long address) {
		return read(address);
	}

	@Override
	public K loadKey(long address) {
		Entry<K, V> entry = load(address);
		return entry.getKey();
	}

	@Override
	public V loadValue(long address) {
		Entry<K, V> entry = load(address);
		return entry.getValue();
	}

	public long allocate(int size) {
		long address = -1;

		for (int i = allocators.size(); i > 0;) {
			Allocator allocator = allocators.get(--i);
			// System.out.println(allocator);
			// System.out.println(allocator);
			address = allocator.allocate(size);
			if (address != -1) {
				// System.out.println(i);
				return toLong(i, (int) address);
			}
		}

		// System.out.println("inflate " + allocators.size());
		// for (int i = 0; i < allocators.size(); i++) {
		// System.out.println(allocators.get(i));
		// }

		// inflate storage
		addPage();
		int last = allocators.size() - 1;
		address = allocators.get(last).allocate(size);
		return toLong(last, (int) address);
	}

	private long toLong(int x, int y) {
		return (((long) x << 32 | (y & 0xffffffffL)));
	}

	private int pageNo(long address) {
		return (int) (address >> 32);
	}

	private int indexFor(int hash) {
		return hash & (tableSize - 1);
	}

	private void safeClose(Closeable c) {
		if (c == null)
			return;

		try {
			c.close();
		} catch (Throwable e) {
		}
	}
}

// public static int toInt(byte bytes[], int s) {
// return ((((int) bytes[s + 0] & 0xff) << 24) | (((int) bytes[s + 1] & 0xff) <<
// 16) | (((int) bytes[s + 2] & 0xff) << 8) | (((int) bytes[s + 3] & 0xff)));
// }
//
// public static long toLong(byte[] data, int s) {
// if (data == null || data.length < 8 + s)
// return 0x0;
//
// return (long) ((long) (0xff & data[0 + s]) << 56 | (long) (0xff & data[1 +
// s]) << 48 | (long) (0xff & data[2 + s]) << 40
// | (long) (0xff & data[3 + s]) << 32 | (long) (0xff & data[4 + s]) << 24 |
// (long) (0xff & data[5 + s]) << 16
// | (long) (0xff & data[6 + s]) << 8 | (long) (0xff & data[7 + s]) << 0);
// }
//
// public static byte[] toBytes(int value) {
// byte[] byteArray = new byte[4];
// byteArray[0] = (byte) (value >> 24);
// byteArray[1] = (byte) (value >> 16);
// byteArray[2] = (byte) (value >> 8);
// byteArray[3] = (byte) (value);
// return byteArray;
// }
//
// public static byte[] toBytes(long value) {
// return new byte[] { (byte) (value >> 56), (byte) (value >> 48), (byte) (value
// >> 40), (byte) (value >> 32),
// (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8), (byte) value
// };
// }

// private byte[] marshal(Entry<K, V> e) {
// byte[] keyBytes = keySerializer.serialize(e.getKey());
// int keyLen = keySerializer.size() == -1 ? keyBytes.length :
// keySerializer.size();
// byte[] valueBytes = valueSerializer.serialize(e.getValue());
// int valueLen = valueSerializer.size() == -1 ? valueBytes.length :
// keySerializer.size();
// int dataSize = sizeOf(keyLen, valueLen);
// return marshal(dataSize, e.getHash(), e.next(), e.getTimeoutTime(), keyLen,
// keyBytes, valueBytes);
// }
//
// private byte[] marshal(int dataSize, int hash, long next, long timeoutTime,
// int keyLen, byte[] keyBytes, byte[] valueBytes) {
// byte[] dataBytes = new byte[dataSize];
// System.arraycopy(toBytes(hash), 0, dataBytes, Entry.Hash, 4);
// System.arraycopy(toBytes(next), 0, dataBytes, Entry.Next, 8);
// System.arraycopy(toBytes(timeoutTime), 0, dataBytes, Entry.Timeout, 8);
// System.arraycopy(toBytes(keyLen), 0, dataBytes, Entry.KeyLen, 4);
// System.arraycopy(keyBytes, 0, dataBytes, Entry.Key, keyBytes.length);
// System.arraycopy(valueBytes, 0, dataBytes, Entry.Key + keyBytes.length,
// valueBytes.length);
// return dataBytes;
// }

// private Entry<K, V> parse(byte[] value, long address) {
// if (value == null || value.length == 0)
// return null;
//
// int hash = toInt(value, Entry.Hash);
// long next = toLong(value, Entry.Next);
// long timeoutTime = toLong(value, Entry.Timeout);
// int keyLen = toInt(value, Entry.KeyLen);
// K k = keySerializer.deserialize(value, Entry.Key, keyLen);
// V v = valueSerializer.deserialize(value, Entry.Key + keyLen, value.length -
// (Entry.Key + keyLen));
// return new Entry<K, V>(k, v, hash, next, timeoutTime, address);
// }
//
// private int sizeOf(int keyLen, int valueLen) {
// return Entry.Key + keyLen + valueLen;
// }

