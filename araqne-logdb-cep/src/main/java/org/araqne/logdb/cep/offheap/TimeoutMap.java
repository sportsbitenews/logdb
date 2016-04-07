package org.araqne.logdb.cep.offheap;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.araqne.logdb.cep.offheap.evict.TimeoutEventListener;

public interface TimeoutMap<K, V> {
	V get(K key);

	void put(K key, V value);

	/* timeoutTime can update */
	void put(K key, V value, String host, long exprireTime, long timeoutTime);

	boolean putIfAbsent(K key, V value, String host, long expireTime, long timeoutTime);

	boolean replace(K key, V oldValue, V newValue, String host, long timeoutTime);

	boolean remove(K key);

	Iterator<K> getKeys();

	void clear();

	void close();

	void setTime(String host, long time);

	long getLastTime(String host);

	Set<String> hostSet();

	List<V> timeoutQueue(String host);

	List<V> expireQueue(String host);

	void addListener(TimeoutEventListener<K, V> listener);

	void removeListener(TimeoutEventListener<K, V> listener);

	void clearClock();
}