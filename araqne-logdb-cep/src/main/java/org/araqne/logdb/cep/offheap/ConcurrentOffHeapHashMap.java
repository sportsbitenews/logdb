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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.araqne.logdb.cep.offheap.engine.StorageEngineFactory;
import org.araqne.logdb.cep.offheap.timeout.OffHeapEventListener;

public class ConcurrentOffHeapHashMap<K, V> implements OffHeapConcurrency<K, V> {

	private Segment<K, V>[] segments;
	private int concurrency = 32; /* Power of two ( x & x-1 == 0) */

	public ConcurrentOffHeapHashMap(StorageEngineFactory<K, V> factory) {
		this(32, factory);
	}

	@SuppressWarnings("unchecked")
	public ConcurrentOffHeapHashMap(int concurrency, StorageEngineFactory<K, V> factory) {
		this.concurrency = concurrency;
		segments = new Segment[concurrency];
		for (int i = 0; i < concurrency; i++) {
			segments[i] = new Segment<K, V>(factory.instance());
		}
	}

	private Segment<K, V> segmentFor(Object key) {
		int mod = key.hashCode() & (concurrency - 1);
		return segments[mod];
	}

	public V putIfAbsent(K key, V value, String host, long expireTime, long timeoutTime) {
		return segmentFor(key).putIfAbsent(key, value, host, expireTime, timeoutTime);
	}

	@Override
	public V putIfAbsent(K key, V value) {
		return segmentFor(key).putIfAbsent(key, value);
	}

	public boolean replace(K key, V oldValue, V newValue) {
		Segment<K, V> s = segmentFor(key);
		return s != null && s.replace(key, oldValue, newValue);
	}

	public boolean replace(K key, V oldValue, V newValue, String host, long timeoutTime) {
		Segment<K, V> s = segmentFor(key);
		return s != null && s.replace(key, oldValue, newValue, host, timeoutTime);
	}

	public void put(K key, V value, String host, long expireTime, long timeoutTime) {
		if (key == null)
			new IllegalArgumentException("key is null");

		segmentFor(key).put(key, value, host, expireTime, timeoutTime);
	}

	@Override
	public void put(K key, V value) {
		if (key == null)
			new IllegalArgumentException("key is null");

		segmentFor(key).put(key, value);
	}

	@Override
	public V get(K key) {
		if (key == null)
			return null;

		return segmentFor(key).get(key);
	}

	@Override
	public boolean remove(K key) {
		return segmentFor(key).remove(key);
	}

	@Override
	public void clear() {
		for (Segment<K, V> segment : segments) {
			segment.clear();
		}
	}

	@Override
	public void close() {
		for (Segment<K, V> segment : segments) {
			try {
				segment.close();
			} catch (Exception e) {
				// TODO log
			}
		}
	}

	@Override
	public Set<String> hostSet() {
		Set<String> hosts = new HashSet<String>();
		for (Segment<K, V> segment : segments) {
			hosts.addAll(segment.hostSet());
		}
		return hosts;
	}

	@Override
	public Iterator<K> getKeys() {
		return new KeyIterator();
	}

	private class KeyIterator implements Iterator<K> {

		int i = 0;
		Iterator<K> itr;

		public KeyIterator() {
			itr = segments[i].getKeys();
		}

		@Override
		public boolean hasNext() {
			if (itr.hasNext())
				return true;

			while (++i < concurrency) {
				itr = segments[i].getKeys();
				if (itr.hasNext())
					return true;
			}
			return false;
		}

		@Override
		public K next() {
			if (itr.hasNext())
				return itr.next();

			while(++i < concurrency) {
				itr = segments[i].getKeys();
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
		for (Segment<K, V> segment : segments) {
			timeoutQueue.addAll(segment.timeoutQueue(host));
		}
		return timeoutQueue;
	}

	@Override
	public List<V> expireQueue(String host) {
		List<V> expireQueue = new ArrayList<V>();
		for (Segment<K, V> segment : segments) {
			expireQueue.addAll(segment.expireQueue(host));
		}
		return expireQueue;
	}

	@Override
	public void setTime(String host, long now) {
		for (Segment<K, V> segment : segments) {
			segment.setTime(host, now);
		}
	}

	@Override
	public long getLastTime(String host) {
		long lastTime = 0L;
		for (Segment<K, V> segment : segments) {
			lastTime = segment.getLastTime(host);
			if (lastTime != 0L)
				break;
		}
		return lastTime;
	}

	@Override
	public void addListener(OffHeapEventListener<K, V> listener) {
		for (Segment<K, V> segment : segments) {
			segment.addListener(listener);
		}
	}

	@Override
	public void removeListener(OffHeapEventListener<K, V> listener) {
		for (Segment<K, V> segment : segments) {
			segment.removeListener(listener);
		}
	}

	@Override
	public void clearClock() {
		for (Segment<K, V> segment : segments) {
			segment.clearClock();
		}
	}
}
