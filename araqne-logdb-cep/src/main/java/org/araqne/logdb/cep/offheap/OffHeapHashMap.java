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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.araqne.logdb.cep.offheap.engine.Entry;
import org.araqne.logdb.cep.offheap.engine.StorageEngine;
import org.araqne.logdb.cep.offheap.timeout.OffHeapEventListener;
import org.araqne.logdb.cep.offheap.timeout.OffHeapMapClock;
import org.araqne.logdb.cep.offheap.timeout.TimeoutItem;
import org.araqne.logdb.cep.offheap.timeout.TimeoutQueue;

public class OffHeapHashMap<K, V> implements OffHeapMap<K, V> {
	private StorageEngine<K, V> engine;
	private OffHeapMapClock<K, V> realClock;
	private ConcurrentHashMap<String, OffHeapMapClock<K, V>> logClocks = new ConcurrentHashMap<String, OffHeapMapClock<K, V>>();;

	public OffHeapHashMap(StorageEngine<K, V> storage) {
		engine = storage;
		realClock = new OffHeapMapClock<K, V>(engine, null);
	}

	protected int hash(Object key) {
		/*
		 * int h = hashSeed; if (0 != h && k instanceof String) { return
		 * sun.misc.Hashing.stringHash32((String) k); }
		 * 
		 * h ^= k.hashCode();
		 * 
		 * // This function ensures that hashCodes that differ only by //
		 * constant multiples at each bit position have a bounded // number of
		 * collisions (approximately 8 at default load factor). h ^= (h >>> 20)
		 * ^ (h >>> 12); return h ^ (h >>> 7) ^ (h >>> 4);
		 */
		if (key == null)
			return 0;

		return key.hashCode();
	}

	@Override
	public V put(K key, V value) {
		return put(key, value, null, 0L);
	}

	@Override
	public V get(Object key) {
		Entry<K, V> entry = getEntry(key);
		return null == entry ? null : entry.getValue();
	}

	private Entry<K, V> getEntry(Object key) {
		int hash = hash(key);
		for (Entry<K, V> e = engine.get(hash); e != null; e = engine.next(e)) {
			if (e.equalsKey(key)) {
				return e;
			}
		}
		return null;
	}

	@Override
	public V remove(Object key) {
		int hash = hash(key);
		Entry<K, V> prev = null;
		for (Entry<K, V> e = engine.get(hash); e != null; e = engine.next(e)) {
			if (e.equalsKey(key)) {
				engine.remove(e, prev);
				return e.getValue();
			}
			prev = e;
		}
		return null;
	}

	@Override
	public Iterator<K> getKeys() {
		return engine.getKeys();
	}

	@Override
	public void clear() {
		// TODO clock쪽도
		engine.clear();
	}

	@Override
	public void close() {
		// TODO clock 등 다시 확인
		try {
			engine.close();
		} catch (Exception e) {
		}
	}

	// ------------------E - X - P - I - R - E ---------------------//
	@Override
	public V put(K key, V value, String host, long expireTime) {
		Entry<K, V> prev = null;
		long address = 0L;
		V oldValue = null;
		int hash = hash(key);
		for (Entry<K, V> e = engine.get(hash); e != null; e = engine.next(e)) {
			if (e.equalsKey(key)) {
				if (expireTime > e.getTimeoutTime()) {
					e.setTimeoutTime(expireTime);
					OffHeapMapClock<K, V> clock = ensureEventClock(host);
					clock.addTimeoutTime(new TimeoutItem(expireTime, address));
				}

				oldValue = e.getValue();
				address = engine.update(e, prev, value);
				hash = e.getHash();
				break;
				// return oldValue;
			}
			prev = e;
		}
		
		if (address == 0L)
			address = engine.add(hash, key, value, expireTime);

		if (expireTime > 0) {
			OffHeapMapClock<K, V> clock = ensureEventClock(host);
			clock.addExpireTime(new TimeoutItem(expireTime, address));
			// addExpireQueue(address, host, expireTime);
		}
		return oldValue;
	}

	@Override
	public void timeout(K key, String host, long timeoutTime) {
		long address = 0L;
		Entry<K, V> prev = null;
		int hash = hash(key);
		for (Entry<K, V> e = engine.get(hash); e != null; e = engine.next(e)) {
			if (e.equalsKey(key)) {
				address = engine.updateTime(e, prev, timeoutTime);
				break;
			}
			prev = e;
		}

		if (address == 0L)
			throw new IllegalArgumentException("key error");

		OffHeapMapClock<K, V> clock = ensureEventClock(host);
		clock.addTimeoutTime(new TimeoutItem(timeoutTime, address));
		// Entry<K, V> entry = engine.load(address);
		// System.out.println("timeout 제대로 업데이트 됐는지 확인 " + entry );
		// System.out.println("actual " + entry.getTimeoutTime() + ", expected "
		// + timeoutTime);
		// return address;
	}

	private OffHeapMapClock<K, V> ensureEventClock(String host) {
		if (host == null) {
			return realClock;
		}

		OffHeapMapClock<K, V> clock = null;
		clock = logClocks.get(host);
		if (clock == null) {
			clock = new OffHeapMapClock<K, V>(engine, host);
			OffHeapMapClock<K, V> oldClock = logClocks.putIfAbsent(host, clock);
			if (oldClock != null) {
				return oldClock;
			} else {
				return clock;
			}
		} else {
			return clock;
		}
	}

	@Override
	public void setTime(String host, long now) {
		OffHeapMapClock<K, V> clock = ensureEventClock(host);
		clock.setTime(now, false);
		//
		// while (true) {
		// // expire time
		// TimeoutItem item = clock.timeoutQueue().peek();
		// if (item == null || item.getTime() > time) {
		// break;
		// } else {
		// engine.evict(clock.timeoutQueue().remove());
		// }
		// }
	}

	@Override
	public List<V> expireQueue(String host) {
		OffHeapMapClock<K, V> clock = ensureEventClock(host);
		if (clock == null)
			return null;

		TimeoutQueue queue = clock.expireQueue();
		List<V> list = new ArrayList<V>(queue.size());
		for (int i = 0; i < queue.size(); i++) {
			long address = queue.get(i).getAddress();
			Entry<K, V> entry = engine.load(address);
			if (entry != null)
				list.add(entry.getValue());
			// else TODO error log 
		}
		return list;
	}

	@Override
	public List<V> timeoutQueue(String host) {
		OffHeapMapClock<K, V> clock = ensureEventClock(host);
		if (clock == null)
			return null;

		TimeoutQueue queue = clock.timeoutQueue();
		List<V> list = new ArrayList<V>(queue.size());
		for (int i = 0; i < queue.size(); i++) {
			long address = queue.get(i).getAddress();
			Entry<K, V> entry = engine.load(address);
			if (entry != null)
				list.add(entry.getValue());
			// else TODO error log 
		}
		return list;
	}

	// @Override
	// public void timeout(K key, long timeoutTime, String host) {
	// // TODO Auto-generated method stub
	// }

	@Override
	public long getLastTime(String host) {
		OffHeapMapClock<K, V> clock = ensureEventClock(host);
		return clock.getDate().getTime();
	}

	@Override
	public Set<String> hostSet() {
		return logClocks.keySet();
	}

	@Override
	public void addListener(OffHeapEventListener<K, V> listener) {
		engine.addListener(listener);
	}

	@Override
	public void removeListener(OffHeapEventListener<K, V> listener) {
		engine.removeListner(listener);
	}

	@Override
	public void clearClock() {
		// TODO Auto-generated method stub
	}
}

// private void addExpireQueue(long address, String host, long expireTime) {
// OffHeapMapClock<K,V> clock = ensureEventClock(host);
// clock.addExpireTime(new TimeoutItem(expireTime, address));
// }
// private Un_ExpireQueue ensureExpireQueue(String host) {
// if (host == null)
// return realExpireQueue;
//
// Un_ExpireQueue queue = null;
// queue = logExpireQeueue.get(host);
// if (queue == null) {
// queue = new Un_ExpireQueue();
// Un_ExpireQueue old = logExpireQeueue.putIfAbsent(host, queue);
// if (old != null)
// return old;
// return queue;
// } else {
// return queue;
// }
// }

// private TimeoutQueue ensureTimeoutQueue(String host) {
// if (host == null)
// return realTimeoutQueue;
//
// TimeoutQueue queue = null;
// queue = logTimeoutQueue.get(host);
// if (queue == null) {
// queue = new TimeoutQueue();
// TimeoutQueue old = logTimeoutQueue.putIfAbsent(host, queue);
// if (old != null)
// return old;
// return queue;
// } else {
// return queue;
// }
// }

// private long address(K key) {
// return 0;
// }
// private Un_ExpireQueue expireQueue(String host) {
// if (host == null)
// return realExpireQueue;
// else
// return ensureExpireQueue(host);
// }
//
// private TimeoutQueue timeoutQueue(String host) {
// if (host == null)
// return realTimeoutQueue;
// else
// return ensureTimeoutQueue(host);
// }

// public void setTime(long time) {
// while (true) {
// // expire time
// TimeoutItem item = realExpireQueue.peek();
// if (item == null || item.getTime() > time) {
// break;
// } else {
// evictValue(realExpireQueue.remove());
// }
// }
// }
