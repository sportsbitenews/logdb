package org.araqne.logdb.cep.offheap.evict;

public interface TimeoutEventListener<K, V> {
	void onTimeout(K key, V value, long expireTime);
}
