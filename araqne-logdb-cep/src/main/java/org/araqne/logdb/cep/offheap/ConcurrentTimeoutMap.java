/*
 * Copyright 2016 Eediom Inc.
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.araqne.logdb.cep.offheap.evict.TimeoutEventListener;
import org.araqne.logdb.cep.offheap.factory.TimeoutMapFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConcurrentTimeoutMap<K, V> implements TimeoutMap<K, V> {
	private final Logger slog = LoggerFactory.getLogger(ConcurrentTimeoutMap.class);

	private final int segmentSize;
	private List<TimeoutMap<K, V>> segments;
	private final int hashSeed = 918217765;

	public ConcurrentTimeoutMap(TimeoutMapFactory<K, V> factory) {
		this(7, factory);
	}

	public ConcurrentTimeoutMap(int concurrencyExponent, TimeoutMapFactory<K, V> factory) {
		if (concurrencyExponent > 10)
			throw new IllegalArgumentException("an exponent of concurrency shuld be less than 10");

		this.segmentSize = 1 << concurrencyExponent; /* Power of two */
		segments = new ArrayList<TimeoutMap<K, V>>(segmentSize);
		for (int i = 0; i < segmentSize; i++) {
			segments.add(i, new LockedOffHeapMap(factory.map()));
		}
	}

	public boolean putIfAbsent(K key, V value, String host, long expireTime, long timeoutTime) {
		return segmentFor(key).putIfAbsent(key, value, host, expireTime, timeoutTime);
	}

	public boolean putIfAbsent(K key, V value) {
		return segmentFor(key).putIfAbsent(key, value, null, 0L, 0L);
	}

	public boolean replace(K key, V oldValue, V newValue) {
		TimeoutMap<K, V> s = segmentFor(key);
		return s != null && s.replace(key, oldValue, newValue, null, 0L);
	}

	public boolean replace(K key, V oldValue, V newValue, String host, long timeoutTime) {
		TimeoutMap<K, V> s = segmentFor(key);
		return s != null && s.replace(key, oldValue, newValue, host, timeoutTime);
	}

	public void put(K key, V value, String host, long expireTime, long timeoutTime) {
		segmentFor(key).put(key, value, host, expireTime, timeoutTime);
	}

	@Override
	public void put(K key, V value) {
		segmentFor(key).put(key, value);
	}

	@Override
	public V get(K key) {
		return segmentFor(key).get(key);
	}

	@Override
	public boolean remove(K key) {
		return segmentFor(key).remove(key);
	}

	@Override
	public void clear() {
		for (TimeoutMap<K, V> segment : segments) {
			segment.clear();
		}
	}

	@Override
	public void close() {
		for (TimeoutMap<K, V> segment : segments) {
			try {
				segment.close();
			} catch (Exception e) {
				slog.warn("araqne logdb cep : failed to close a segment", e);
			}
		}
	}

	@Override
	public Set<String> hostSet() {
		Set<String> hosts = new HashSet<String>();
		for (TimeoutMap<K, V> segment : segments) {
			hosts.addAll(segment.hostSet());
		}
		return hosts;
	}

	@Override
	public Iterator<K> getKeys() {
		return new KeyIterator();
	}

	private class KeyIterator implements Iterator<K> {
		private int i = 0;
		private Iterator<K> itr = segments.get(0).getKeys();

		@Override
		public boolean hasNext() {
			if (itr.hasNext())
				return true;

			while (++i < segments.size()) {
				itr = segments.get(i).getKeys();
				if (itr.hasNext())
					return true;
			}
			return false;
		}

		@Override
		public K next() {
			if (itr.hasNext())
				return itr.next();

			while (++i < segments.size()) {
				itr = segments.get(i).getKeys();
				if (itr.hasNext())
					return itr.next();
			}
			return null;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	@Override
	public List<V> timeoutQueue(String host) {
		List<V> timeoutQueue = new ArrayList<V>();
		for (TimeoutMap<K, V> segment : segments) {
			timeoutQueue.addAll(segment.timeoutQueue(host));
		}
		return timeoutQueue;
	}

	@Override
	public List<V> expireQueue(String host) {
		List<V> expireQueue = new ArrayList<V>();
		for (TimeoutMap<K, V> segment : segments) {
			expireQueue.addAll(segment.expireQueue(host));
		}
		return expireQueue;
	}

	@Override
	public void setTime(String host, long now) {
		for (TimeoutMap<K, V> segment : segments) {
			segment.setTime(host, now);
		}
	}

	@Override
	public long getLastTime(String host) {
		long lastTime = 0L;
		for (TimeoutMap<K, V> segment : segments) {
			lastTime = segment.getLastTime(host);
			if (lastTime != 0L)
				break;
		}
		return lastTime;
	}

	@Override
	public void addListener(TimeoutEventListener<K, V> listener) {
		for (TimeoutMap<K, V> segment : segments) {
			segment.addListener(listener);
		}
	}

	@Override
	public void removeListener(TimeoutEventListener<K, V> listener) {
		for (TimeoutMap<K, V> segment : segments) {
			segment.removeListener(listener);
		}
	}

	@Override
	public void clearClock() {
		for (TimeoutMap<K, V> segment : segments) {
			segment.clearClock();
		}
	}

	private TimeoutMap<K, V> segmentFor(Object key) {
		if (key == null)
			new IllegalArgumentException("key is null");

		int mod = hash(key) & (segmentSize - 1);
		return segments.get(mod);
	}

	private int hash(Object k) {
		int h = hashSeed;
		h ^= k.hashCode();

		// Spread bits to regularize both segment and index locations,
		// using variant of single-word Wang/Jenkins hash.
		h += (h << 15) ^ 0xffffcd7d;
		h ^= (h >>> 10);
		h += (h << 3);
		h ^= (h >>> 6);
		h += (h << 2) + (h << 14);
		return h ^ (h >>> 16);
	}

	private class LockedOffHeapMap implements TimeoutMap<K, V> {
		private TimeoutMap<K, V> map;

		private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
		private final Lock write = lock.writeLock();
		private final Lock read = lock.readLock();

		public LockedOffHeapMap(TimeoutMap<K, V> map) {
			this.map = map;
		}

		public void put(K key, V value) {
			write.lock();
			try {
				map.put(key, value);
			} finally {
				write.unlock();
			}
		}

		public V get(K key) {
			read.lock();
			try {
				return map.get(key);
			} finally {
				read.unlock();
			}
		}

		public boolean remove(K key) {
			write.lock();
			try {
				return map.remove(key);
			} finally {
				write.unlock();
			}
		}

		public Iterator<K> getKeys() {
			read.lock();
			try {
				return map.getKeys();
			} finally {
				read.unlock();
			}
		}

		public void clear() {
			write.lock();
			try {
				map.clear();
			} finally {
				write.unlock();
			}
		}

		public void close() {
			write.lock();
			try {
				map.close();
			} finally {
				write.unlock();
			}
		}

		public void put(K key, V value, String host, long expireTime, long updateTime) {
			write.lock();
			try {
				map.put(key, value, host, expireTime, updateTime);
			} finally {
				write.unlock();
			}
		}

		public void setTime(String host, long now) {
			write.lock();
			try {
				map.setTime(host, now);
			} finally {
				write.unlock();
			}
		}

		public boolean replace(K key, V oldValue, V newValue, String host, long timeoutTime) {
			write.lock();
			try {
				return map.replace(key, oldValue, newValue, host, timeoutTime);
			} finally {
				write.unlock();
			}
		}

		public boolean putIfAbsent(K key, V value, String host, long expireTime, long timeoutTime) {
			write.lock();
			try {
				return map.putIfAbsent(key, value, host, expireTime, timeoutTime);
			} finally {
				write.unlock();
			}
		}

		@Override
		public void addListener(TimeoutEventListener<K, V> listener) {
			write.lock();
			try {
				map.addListener(listener);
			} finally {
				write.unlock();
			}
		}

		@Override
		public void removeListener(TimeoutEventListener<K, V> listener) {
			write.lock();
			try {
				map.removeListener(listener);
			} finally {
				write.unlock();
			}
		}

		@Override
		public List<V> timeoutQueue(String host) {
			read.lock();
			try {
				return map.timeoutQueue(host);
			} finally {
				read.unlock();
			}
		}

		@Override
		public List<V> expireQueue(String host) {
			read.lock();
			try {
				return map.expireQueue(host);
			} finally {
				read.unlock();
			}
		}

		@Override
		public long getLastTime(String host) {
			read.lock();
			try {
				return map.getLastTime(host);
			} finally {
				read.unlock();
			}
		}

		@Override
		public Set<String> hostSet() {
			read.lock();
			try {
				return map.hostSet();
			} finally {
				read.unlock();
			}
		}

		@Override
		public void clearClock() {
			write.lock();
			try {
				map.clearClock();
			} finally {
				write.unlock();
			}
		}
	}

}
