package org.araqne.logdb.cep.offheap.engine;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.araqne.logdb.cep.offheap.engine.serialize.Serialize;
import org.araqne.logdb.cep.offheap.storage.EntryStorageManager;
import org.araqne.logdb.cep.offheap.storage.UnsafeLongStorageArea;
import org.araqne.logdb.cep.offheap.timeout.OffHeapEventListener;
import org.araqne.logdb.cep.offheap.timeout.TimeoutItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReferenceStorageEngine<K, V> implements StorageEngine<K, V> {
	private final Logger slog = LoggerFactory.getLogger(ReferenceStorageEngine.class);
	private final static long NULL_ADDRESS = 0L;
	private final static int defaultIndexSize = 1024;// 1024*1024*16;// 2^24
	private final static int defaultChunkSize = 1024 * 4;

	private int tableSize;
	private UnsafeLongStorageArea indexTable;
	private EntryStorageManager<K, V> manager;
	private List<OffHeapEventListener<K, V>> listeners = new ArrayList<OffHeapEventListener<K, V>>();

	public ReferenceStorageEngine(Serialize<K> keySerializer, Serialize<V> valueSerializer) {
		this(defaultIndexSize, defaultChunkSize, keySerializer, valueSerializer);
	}

	public ReferenceStorageEngine(int indexSize, Serialize<K> keySerializer, Serialize<V> valueSerializer) {
		this(indexSize, defaultChunkSize, keySerializer, valueSerializer);
	}

	public ReferenceStorageEngine(int indexSize, int chunkSize, Serialize<K> keySerializer, Serialize<V> valueSerializer) {
		this.tableSize = indexSize;
		this.indexTable = new UnsafeLongStorageArea(indexSize);
		this.manager = new EntryStorageManager<K, V>(chunkSize, keySerializer, valueSerializer);
	}

	/*
	 * address위치에 저장된 entry를 가져온다.
	 */
	@Override
	public Entry<K, V> getEntry(long address) {
		if (address == NULL_ADDRESS)
			return null;
		return manager.get(address);
	}

	/**
	 * address에 저장된 entry의 value를 가져온다 -
	 */
	@Override
	public V getValue(long address) {
		if (address == NULL_ADDRESS)
			return null;

		return manager.getValue(address);
	}

	/**
	 * 새로운 k-value를 저정한다.
	 */
	@Override
	public long add(int hash, K key, V value) {
		return add(hash, key, value, 0L);
	}

	@Override
	public long add(int hash, K key, V value, long timeoutTime) {
		int index = indexFor(hash);
		long nextEntry = indexTable.getValue(index);
		Entry<K, V> entry = new Entry<K, V>(key, value, hash, nextEntry, timeoutTime);
		long address = manager.putEntry(entry);
		indexTable.setValue(index, address);
		return address;
	}

	/**
	 * value만 update
	 */
	@Override
	public long replace(int hash, long address, V value, long timeoutTime) {
		if (address == NULL_ADDRESS)
			return address;

		long newAddress = manager.replaceValue(address, value, timeoutTime);

		if (address != newAddress)
			indexTable.setValue(indexFor(hash), newAddress);

		return newAddress;
	}

	/**
	 * hash와 key가 일치하는 entry의 주소를 찾는다.
	 */
	@Override
	public long findAddress(int hash, K key) {
		long address = indexTable.getValue(indexFor(hash));
		for (; address != NULL_ADDRESS; address = manager.getNext(address)) {
			if (manager.equalsHash(address, hash)) {
				if (manager.equalsKey(address, key)) {
					return address;
				}
			}
		}
		return 0L;
	}

	/*
	 * entry list에서 해당 entry를 삭제한다.
	 */
	@Override
	public boolean remove(int hash, K key) {
		long address = indexTable.getValue(indexFor(hash));
		long prev = NULL_ADDRESS;
		for (; address != NULL_ADDRESS; address = manager.getNext(address)) {
			if (manager.equalsHash(address, hash)) {
				if (manager.equalsKey(address, key)) {
					// remove
					/* head 일 때 */
					if (prev == NULL_ADDRESS) {
						indexTable.setValue(indexFor(hash), manager.getNext(address));
					} else {
						manager.setNext(prev, manager.getNext(address));
					}
					manager.remove(address);
					return true;
				}
			}
			prev = address;
		}
		return false;
	}

	/*
	 * entry의 timeout time 과 item의 timeout time이 일치하면 삭제
	 */
	@Override
	public void evict(TimeoutItem item) {
		if (item.getTime() != manager.getTimeoutTime(item.getAddress()))
			return;

		int hash = manager.getHash(item.getAddress());
		long address = indexTable.getValue(indexFor(hash));
		long prev = NULL_ADDRESS;
		for (; address != NULL_ADDRESS; address = manager.getNext(address)) {
			if (address == item.getAddress()) {
				if (prev == NULL_ADDRESS) {
					indexTable.setValue(indexFor(hash), manager.getNext(address));
				} else {
					manager.setNext(prev, manager.getNext(address));
				}
				manager.remove(address);
				return;
			}
			prev = address;
		}
		return;
	}

	public void addListener(OffHeapEventListener<K, V> listener) {
		listeners.add(listener);
	}

	public void removeListner(OffHeapEventListener<K, V> listener) {
		listeners.remove(listener);
	}

	@Override
	public void clear() {
		safeClose(indexTable);
		indexTable = new UnsafeLongStorageArea(tableSize);
		
		manager.clear();
	}

	@Override
	public void close() {
		safeClose(indexTable);
		indexTable = null;
		manager.clear();
	}

	@Override
	public Iterator<K> getKeys() {
		return new KeyIterator();
	}

	private class KeyIterator implements Iterator<K> {
		int index = 0;
		Entry<K, V> entry = null;

		@Override
		public boolean hasNext() {
			if (entry != null && entry.getNext() != NULL_ADDRESS) {
				return true;
			}

			// 대부분은 entry.next가 null일
			int tempIndex = index;
			while (tempIndex < tableSize) {
				long address = indexTable.getValue(tempIndex++);
				if (address == NULL_ADDRESS)
					continue;

				index = tempIndex - 1;
				return true;
			}
			return false;
		}

		private Entry<K, V> nextEntry(Entry<K, V> entry) {
			if (entry != null && entry.getNext() != NULL_ADDRESS) {
				return getEntry(entry.getNext());
			}

			while (index < tableSize) {
				long address = indexTable.getValue(index++);
				if (address == NULL_ADDRESS)
					continue;

				return getEntry(address);
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

	private int indexFor(int keyHash) {
		return keyHash & (tableSize - 1);
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
