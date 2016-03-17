package org.araqne.logdb.cep.offheap.engine;

import java.io.Closeable;
import java.util.Iterator;

import org.araqne.logdb.cep.offheap.timeout.OffHeapEventListener;
import org.araqne.logdb.cep.offheap.timeout.TimeoutItem;

public interface StorageEngine<K, V> extends Closeable {
	Entry<K, V> get(int i);

	Entry<K, V> next(Entry<K, V> i);

	long add(int hash, K key, V value);

	Entry<K, V> remove(Entry<K, V> t, Entry<K, V> p);

	long update(Entry<K, V> t, Entry<K, V> p, V v);

	void clear();

	Iterator<K> getKeys();

	Entry<K, V> load(long address);

	K loadKey(long address);

	V loadValue(long address);

	// Iterator<V> getValue();

	long add(int hash, K key, V value, long expireTime);

	// long timeout(long address);

	void evict(TimeoutItem item);

	long updateTime(Entry<K, V> oldValue, Entry<K, V> prev, long timeoutTime);
	
	void addListener(OffHeapEventListener<K,V> listener); 
	
	void removeListner(OffHeapEventListener<K,V> listener); 

}
