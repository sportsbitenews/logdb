package org.araqne.logdb.cep.offheap.manager;

import org.araqne.logdb.cep.offheap.engine.Entry;

public interface EntryStorageManager<K, V> {

	long set(Entry<K, V> entry);

	void setHash(long address, int hash);

	void setNext(long address, long next);

	void setEvictTime(long address, long timeoutTime);

	void setMaxSize(long address, int maxSize);

	void setKey(long address, K key);

	void setKeySize(long address, int keySize);

	void setValue(long address, V value);

	void setValueSize(long address, int valueSize);

	Entry<K, V> get(long address);

	int getHash(long address);

	long getNext(long address);

	long getEvictTime(long address);

	int getKeySize(long address);

	K getKey(long address);

	V getValue(long address);

	long updateValue(long address, V value, long timeoutTime);

	long replaceValue(long address, V oldValue, V newValue, long timeoutTime);

	boolean equalsHash(long address, int hash);

	boolean equalsKey(long address, K key);

	void remove(long address);
}
