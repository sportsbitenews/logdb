package org.araqne.logdb.cep.offheap.engine;

import java.io.Closeable;
import java.util.Iterator;

import org.araqne.logdb.cep.offheap.evict.TimeoutEventListener;
import org.araqne.logdb.cep.offheap.evict.EvictItem;

public interface Storage<K, V> extends Closeable {

	long add(K key, V value);

	long add(K key, V value, long expireTime);

	V getValue(long address);

	Entry<K, V> getEntry(long address);

	long findAddress(K key);

	boolean remove(K key);

	long updateValue(long address, K key, V value, long timeoutTime);

	long replace(long address, K key, V oldValue, V newValue, long timeoutTime);

	void evict(EvictItem item);

	Iterator<K> getKeys();

	void addListener(TimeoutEventListener<K, V> listener);

	void removeListner(TimeoutEventListener<K, V> listener);

	void clear();
}
