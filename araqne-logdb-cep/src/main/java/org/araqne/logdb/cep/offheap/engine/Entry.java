package org.araqne.logdb.cep.offheap.engine;

public class Entry<K, V> {
	private K key;
	private V value;
	private int hash;
	private long next;
	private long evictTime;
	private int maxSize;

	public Entry(K key, V value, int hash, long next, long time) {
		this(key, value, hash, next, time, 0);
	}

	public Entry(K key, V value, int hash, long next, long time, int maxSize) {
		this.key = key;
		this.value = value;
		this.hash = hash;
		this.next = next;
		this.evictTime = time;
		this.maxSize = maxSize;
	}

	public int getHash() {
		return hash;
	}

	public void setHash(int hash) {
		this.hash = hash;
	}

	public K getKey() {
		return key;
	}

	public V getValue() {
		return value;
	}

	public V setValue(V v) {
		V oldValue = value;
		value = v;
		return oldValue;
	}

	public long getNext() {
		return next;
	}

	public void setNext(long next) {
		this.next = next;
	}

	public long getEvictTime() {
		return evictTime;
	}

	public void setEvictTime(long time) {
		this.evictTime = time;
	}

	public int getMaxSize() {
		return maxSize;
	}

	public void setMaxSize(int maxSize) {
		this.maxSize = maxSize;
	}

	public String toString() {
		return "key = " + key + ", value = " + value + ", hash = " + hash + ", next address = " + next
				+ ", evict time = " + evictTime + ", max size = " + maxSize;
	}
}
