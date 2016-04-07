package org.araqne.logdb.cep.offheap.engine;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.araqne.logdb.cep.offheap.evict.EvictItem;
import org.araqne.logdb.cep.offheap.evict.TimeoutEventListener;
import org.araqne.logdb.cep.offheap.manager.EntryStorageManager;
import org.araqne.logdb.cep.offheap.storageArea.UnsafeLongStorageArea;

public class ReferenceStorage<K, V> implements Storage<K, V> {
	private final static long NULL_ADDRESS = 0L;
	private final static int defaultIndexSize = 1024;

	private int indexTableSize;
	private UnsafeLongStorageArea indexTable;
	private EntryStorageManager<K, V> manager;
	private List<TimeoutEventListener<K, V>> listeners = new ArrayList<TimeoutEventListener<K, V>>();

	public ReferenceStorage(EntryStorageManager<K, V> manager) {
		this(defaultIndexSize, manager);
	}

	public ReferenceStorage(int indexSize, EntryStorageManager<K, V> manager) {
		this.indexTableSize = indexSize;
		this.indexTable = new UnsafeLongStorageArea(indexSize);
		this.manager = manager;
	}

	private int hash(Object key) {
		if (key == null)
			return 0;

		return key.hashCode();
	}

	@Override
	public Entry<K, V> getEntry(long address) {
		if (address == NULL_ADDRESS)
			return null;

		return manager.get(address);
	}

	@Override
	public V getValue(long address) {
		if (address == NULL_ADDRESS)
			return null;

		return manager.getValue(address);
	}

	@Override
	public long add(K key, V value) {
		return add(key, value, 0L);
	}

	@Override
	public long add(K key, V value, long timeoutTime) {
		if (indexTable == null)
			throw new IllegalStateException("a storage was closed");

		if (key == null)
			throw new IllegalArgumentException("key cannot be null");

		int hash = hash(key);
		int index = indexFor(hash);
		long nextEntry = indexTable.getValue(index);
		Entry<K, V> entry = new Entry<K, V>(key, value, hash, nextEntry, timeoutTime);
		long address = manager.set(entry);
		indexTable.setValue(index, address);
		return address;
	}

	@Override
	public long updateValue(long address, K key, V value, long timeoutTime) {
		if (indexTable == null)
			throw new IllegalStateException("a storage was closed");

		if (address == NULL_ADDRESS)
			return address;

		long newAddress = manager.updateValue(address, value, timeoutTime);

		if (address != newAddress)
			indexTable.setValue(indexFor(hash(key)), newAddress);

		return newAddress;
	}

	/**
	 * @return address. if not replaced return 0
	 */
	@Override
	public long replace(long address, K key, V oldValue, V newValue, long timeoutTime) {
		if (indexTable == null)
			throw new IllegalStateException("a storage was closed");

		if (address == NULL_ADDRESS)
			return address;

		long newAddress = manager.replaceValue(address, oldValue, newValue, timeoutTime);
		if (newAddress != NULL_ADDRESS && address != newAddress)
			indexTable.setValue(indexFor(hash(key)), newAddress);

		return newAddress;
	}

	@Override
	public long findAddress(K key) {
		if (indexTable == null)
			throw new IllegalStateException("a storage was closed");

		if (key == null)
			throw new IllegalArgumentException("key cannot be null");

		int hash = hash(key);
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

	@Override
	public boolean remove(K key) {
		if (indexTable == null)
			throw new IllegalStateException("a storage was closed");

		if (key == null)
			throw new IllegalArgumentException("key cannot be null");

		int hash = hash(key);
		long address = indexTable.getValue(indexFor(hash));
		long prev = NULL_ADDRESS;
		for (; address != NULL_ADDRESS; address = manager.getNext(address)) {
			if (manager.equalsHash(address, hash)) {
				if (manager.equalsKey(address, key)) {
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

	@Override
	public void evict(EvictItem item) {
		if (indexTable == null)
			throw new IllegalStateException("a storage was closed");

		if (item.getTime() != manager.getEvictTime(item.getAddress()))
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

				Entry<K, V> entry = null;
				for (TimeoutEventListener<K, V> listener : listeners) {
					if (entry == null)
						entry = manager.get(address);
					listener.onTimeout(entry.getKey(), entry.getValue(), entry.getEvictTime());
				}

				manager.remove(address);
				return;
			}
			prev = address;
		}
		return;
	}

	public void addListener(TimeoutEventListener<K, V> listener) {
		if (listeners == null)
			throw new IllegalStateException("a storage was closed");

		listeners.add(listener);
	}

	public void removeListner(TimeoutEventListener<K, V> listener) {
		if (listeners == null)
			throw new IllegalStateException("a storage was closed");

		listeners.remove(listener);
	}

	@Override
	public void clear() {
		if (indexTable == null)
			throw new IllegalStateException("a storage was closed");

		for (int i = 0; i < indexTableSize; i++) {
			removeListOfAddresses(indexTable.getValue(i));
		}

		safeClose(indexTable);
		indexTable = new UnsafeLongStorageArea(indexTableSize);
	}

	private void removeListOfAddresses(long address) {
		while (address != 0L) {
			long next = manager.getNext(address);
			manager.remove(address);
			address = next;
		}
	}

	@Override
	public void close() {
		if (indexTable == null)
			throw new IllegalStateException("a storage was closed");

		for (int i = 0; i < indexTableSize; i++) {
			removeListOfAddresses(indexTable.getValue(i));
		}

		safeClose(indexTable);
		indexTable = null;
		listeners = null;
	}

	@Override
	public Iterator<K> getKeys() {
		if (indexTable == null)
			throw new IllegalStateException("a storage was closed");

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

			int tempIndex = index;
			while (tempIndex < indexTableSize) {
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

			while (index < indexTableSize) {
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
		return keyHash & (indexTableSize - 1);
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
