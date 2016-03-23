package org.araqne.logdb.cep.offheap;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.araqne.logdb.cep.offheap.timeout.OffHeapEventListener;

public interface OffHeapMap<K, V> {
	void put(K key, V value);

	V get(K key);

	boolean remove(K key);

	Iterator<K> getKeys();

	void clear();

	void close();

	void put(K key, V value, String host, long exprireTime, long timeoutTime);

	void setTime(String host, long time);

	long getLastTime(String host);

	Set<String> hostSet();

	List<V> timeoutQueue(String host);

	List<V> expireQueue(String host);

	void addListener(OffHeapEventListener<K, V> listener);

	void removeListener(OffHeapEventListener<K, V> listener);

	void clearClock();
}
