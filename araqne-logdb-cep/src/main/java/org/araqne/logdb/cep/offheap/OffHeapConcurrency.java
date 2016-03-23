package org.araqne.logdb.cep.offheap;

public interface OffHeapConcurrency<K,V>  extends OffHeapMap<K,V>{
	V putIfAbsent(K k, V v);
	
	V putIfAbsent(K key, V value, String host, long expireTime, long timeoutTime);
	
	boolean replace(K k, V old, V v);
}
