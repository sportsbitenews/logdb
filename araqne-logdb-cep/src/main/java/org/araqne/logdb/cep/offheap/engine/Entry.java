package org.araqne.logdb.cep.offheap.engine;

public class Entry<K, V> {
	private K key;
	private V value;
	private int hash;
	private long next;
	private long timeoutTime;
	private int maxSize;
	
	public Entry(K key, V value, int hash, long next, long timeoutTime) {
		this(key, value, hash, next, timeoutTime, 0);
	}

	public Entry(K key, V value, int hash, long next, long timeoutTime, int maxSize) {
		this.key = key;
		this.value = value;
		this.hash = hash;
		this.next = next;
		this.timeoutTime = timeoutTime;
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

	public long getTimeoutTime() {
		return timeoutTime;
	}

	public void setTimeoutTime(long timeoutTime) {
		this.timeoutTime = timeoutTime;
	}

	public int getMaxSize() {
		return maxSize;
	}
	
	public void setMaxSize(int maxSize) {
		this.maxSize = maxSize;
	}

	public String toString() {
		return "key = " + key + ", value = " + value + ", hash = " + hash + ", next address = " + next
				+ ", timeout time = " + timeoutTime + ", max size = " + maxSize;
	}
}
