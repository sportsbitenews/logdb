package org.araqne.logdb.cep.offheap.timeout;

public interface OffHeapEventListener<K, V> {
	void onExpire(K key, V value, long expireTime);
}
