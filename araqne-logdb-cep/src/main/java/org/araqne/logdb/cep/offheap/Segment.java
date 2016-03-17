/*
 * Copyright 2015 Eediom Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.araqne.logdb.cep.offheap;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.araqne.logdb.cep.offheap.engine.StorageEngine;
import org.araqne.logdb.cep.offheap.timeout.OffHeapEventListener;

public class Segment<K, V> extends OffHeapHashMap<K, V> {
	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private final Lock write = lock.writeLock();
	private final Lock read = lock.readLock();

	public Segment(StorageEngine<K, V> storage) {
		super(storage);
	}

	// public void lock() {
	// write.lock();
	// }
	//
	// public void unlock() {
	// write.unlock();
	// }

	public V put(K key, V value) {
		write.lock();
		try {
			return super.put(key, value);
		} finally {
			write.unlock();
		}
	}

	public V get(Object key) {
		read.lock();
		try {
			return super.get(key);
		} finally {
			read.unlock();
		}
	}

	public V remove(Object key) {
		write.lock();
		try {
			return super.remove(key);
		} finally {
			write.unlock();
		}
	}

	public Iterator<K> getKeys() {
		read.lock();
		try {
			return super.getKeys();
		} finally {
			read.unlock();
		}
	}

	public void clear() {
		write.lock();
		try {
			super.clear();
		} finally {
			write.unlock();
		}
	}

	public void close() {
		// Lock write = lock.writeLock();
		write.lock();
		try {
			super.close();
		} finally {
			write.unlock();
		}
	}

	public V put(K key, V value, String host, long expireTime) {
		write.lock();
		try {
			return super.put(key, value, host, expireTime);
		} finally {
			write.unlock();
		}
	}

	public void timeout(K key, String host, long timeoutTime) {
		write.lock();
		try {
			super.timeout(key, host, timeoutTime);
		} finally {
			write.unlock();
		}
	}

	public void setTime(String host, long now) {
		write.lock();
		try {
			super.setTime(host, now);
		} finally {
			write.unlock();
		}
	}

	public boolean replace(K key, V oldValue, V newValue) {
		write.lock();
		boolean replaced = false;
		try {
			if (oldValue.equals(super.get(key))) {
				super.put(key, newValue);
				replaced = true;
			}
		} finally {
			write.unlock();
		}
		return replaced;
	}

	public boolean replace(K key, V oldValue, V newValue, String host, long timeout) {
		write.lock();
		boolean replaced = false;
		try {
			if (oldValue.equals(super.get(key))) {
				super.put(key, newValue, host, timeout);
				replaced = true;
			}
		} finally {
			write.unlock();
		}
		return replaced;
	}

	public V putIfAbsent(K key, V value) {
		write.lock();
		try {
			if (key == null || value == null) {
				throw new NullPointerException();
			}
			V existing = super.get(key);
			if (existing == null) {
				super.put(key, value);
			}
			return existing;
		} finally {
			write.unlock();
		}
	}

	public V putIfAbsent(K key, V value, String host, long timeout) {
		write.lock();
		try {
			if (key == null || value == null) {
				throw new NullPointerException();
			}
			V existing = super.get(key);
			if (existing == null) {
				super.put(key, value, host, timeout);
			}
			return existing;
		} finally {
			write.unlock();
		}
	}

	@Override
	public void addListener(OffHeapEventListener<K, V> listener) {
		write.lock();
		try {
			super.addListener(listener);
		} finally {
			write.unlock();
		}
	}

	@Override
	public void removeListener(OffHeapEventListener<K, V> listener) {
		write.lock();
		try {
			super.removeListener(listener);
		} finally {
			write.unlock();
		}
	}

	@Override
	public List<V> timeoutQueue(String host) {
		read.lock();
		try {
			return super.timeoutQueue(host);
		} finally {
			read.unlock();
		}
	}

	@Override
	public List<V> expireQueue(String host) {
		read.lock();
		try {
			return super.expireQueue(host);
		} finally {
			read.unlock();
		}
	}
}
