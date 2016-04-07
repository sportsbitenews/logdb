package org.araqne.logdb.cep.offheap.engine;

import java.io.IOException;
import java.util.Iterator;

import org.araqne.logdb.cep.offheap.evict.EvictItem;
import org.araqne.logdb.cep.offheap.evict.TimeoutEventListener;

public class FixedLengthKeyStorage<K, V> implements Storage<K, V> {

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
	}

	@Override
	public long add(K key, V value) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long add(K key, V value, long expireTime) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public V getValue(long address) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Entry<K, V> getEntry(long address) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long findAddress(K key) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean remove(K key) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void evict(EvictItem item) {
		// TODO Auto-generated method stub

	}

	@Override
	public Iterator<K> getKeys() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void addListener(TimeoutEventListener<K, V> listener) {
		// TODO Auto-generated method stub

	}

	@Override
	public void removeListner(TimeoutEventListener<K, V> listener) {
		// TODO Auto-generated method stub

	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub

	}

	@Override
	public long updateValue(long address, K key, V value, long timeoutTime) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long replace(long address, K key, V oldValue, V newValue, long timeoutTime) {
		// TODO Auto-generated method stub
		return 0;
	}

}
