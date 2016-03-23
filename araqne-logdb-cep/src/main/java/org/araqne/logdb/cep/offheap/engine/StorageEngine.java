package org.araqne.logdb.cep.offheap.engine;

import java.io.Closeable;
import java.util.Iterator;

import org.araqne.logdb.cep.offheap.timeout.OffHeapEventListener;
import org.araqne.logdb.cep.offheap.timeout.TimeoutItem;

public interface StorageEngine<K, V> extends Closeable {

	long add(int hash, K key, V value);

	long add(int hash, K key, V value, long expireTime);

	V getValue(long address);

	Entry<K, V> getEntry(long address);

	long findAddress(int hash, K key);

	boolean remove(int hash, K key);

	long replace(int hash, long address, V value, long timeoutTime);

	void evict(TimeoutItem item);

	Iterator<K> getKeys();

	void addListener(OffHeapEventListener<K, V> listener);

	void removeListner(OffHeapEventListener<K, V> listener);

	void clear();

}

