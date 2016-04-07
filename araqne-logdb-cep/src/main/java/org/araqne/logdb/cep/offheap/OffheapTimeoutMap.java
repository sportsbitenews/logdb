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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.araqne.logdb.cep.offheap.engine.Entry;
import org.araqne.logdb.cep.offheap.engine.Storage;
import org.araqne.logdb.cep.offheap.evict.EvictItem;
import org.araqne.logdb.cep.offheap.evict.EvictQueue;
import org.araqne.logdb.cep.offheap.evict.TimeoutEventListener;
import org.araqne.logdb.cep.offheap.evict.TimeoutMapClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OffheapTimeoutMap<K, V> implements TimeoutMap<K, V> {
	private final Logger slog = LoggerFactory.getLogger(OffheapTimeoutMap.class);

	private Storage<K, V> storage;
	private TimeoutMapClock<K, V> realClock;
	private ConcurrentHashMap<String, TimeoutMapClock<K, V>> logClocks = new ConcurrentHashMap<String, TimeoutMapClock<K, V>>();;

	public OffheapTimeoutMap(Storage<K, V> storage) {
		this.storage = storage;
		realClock = new TimeoutMapClock<K, V>(storage, null);
	}

	@Override
	public V get(K key) {
		long address = storage.findAddress(key);
		if (address == 0L)
			return null;

		return storage.getValue(address);
	}

	@Override
	public void put(K key, V value) {
		put(key, value, null, 0L, 0L);
	}

	@Override
	public void put(K key, V value, String host, long expireTime, long timeoutTime) {
		put(key, value, host, expireTime, timeoutTime, false);
	}

	@Override
	public boolean putIfAbsent(K key, V value, String host, long expireTime, long timeoutTime) {
		return put(key, value, host, expireTime, timeoutTime, true);
	}

	private boolean put(K key, V value, String host, long expireTime, long timeoutTime, boolean onlyifAbsent) {
		if (key == null)
			throw new IllegalArgumentException("key cannot be null");

		long address = storage.findAddress(key);

		if (address == 0L) { /* first time */
			long time = getMinValue(expireTime, timeoutTime);

			if (slog.isDebugEnabled())
				slog.debug("araqne logdb cep: put in offheap storage - key [{}], value [{}], time [{}]", new Object[] {
						key, value, time });

			address = storage.add(key, value, time);
			if (time > 0) {
				TimeoutMapClock<K, V> clock = ensureEventClock(host);
				if (time == expireTime) {
					clock.addExpireTime(new EvictItem(time, address));

					if (slog.isDebugEnabled())
						slog.debug("araqne logdb cep: queue expire time - key [{}], time [{}], host [{}]",
								new Object[] { key, time, host });
				}
				if (time == timeoutTime) {
					clock.addTimeoutTime(new EvictItem(time, address));

					if (slog.isDebugEnabled())
						slog.debug("araqne logdb cep: queue timeout time - key [{}], time [{}],  host [{}]",
								new Object[] { key, time, host });
				}
			}
		} else { /* update */
			if (onlyifAbsent)
				return false;

			if (slog.isDebugEnabled())
				slog.debug("araqne logdb cep: updated value in offheap storage - key [{}], value [{}], time [{}]",
						new Object[] { key, value, timeoutTime });

			storage.updateValue(address, key, value, timeoutTime);
			if (timeoutTime > 0) {
				TimeoutMapClock<K, V> clock = ensureEventClock(host);
				clock.addTimeoutTime(new EvictItem(timeoutTime, address));

				if (slog.isDebugEnabled())
					slog.debug("araqne logdb cep: set timeout time - key [{}], time [{}], host [{}]", new Object[] {
							key, timeoutTime, host });
			}
		}
		return true;
	}

	@Override
	public boolean replace(K key, V oldValue, V newValue, String host, long timeoutTime) {
		long address = storage.findAddress(key);
		if (storage.replace(address, key, oldValue, newValue, timeoutTime) == 0L)
			return false;

		if (timeoutTime > 0) {
			TimeoutMapClock<K, V> clock = ensureEventClock(host);
			clock.addTimeoutTime(new EvictItem(timeoutTime, address));

			if (slog.isDebugEnabled())
				slog.debug("araqne logdb cep: set timeout time - key [{}], time [{}], host [{}]", new Object[] { key,
						timeoutTime, host });
		}
		return true;
	}

	/**
	 * @return a smaller number larger than 0.
	 */
	private long getMinValue(long a, long b) {
		long min = Math.min(a, b);

		if (min > 0)
			return min;
		else
			return Math.max(a, b);
	}

	@Override
	public boolean remove(K key) {
		return storage.remove(key);
	}

	@Override
	public Iterator<K> getKeys() {
		return storage.getKeys();
	}

	@Override
	public void clear() {
		clearClock();
		storage.clear();
	}

	@Override
	public void close() {
		clearClock();
		try {
			storage.close();
		} catch (Exception e) {
		}
	}

	private TimeoutMapClock<K, V> ensureEventClock(String host) {
		if (host == null) {
			return realClock;
		}

		TimeoutMapClock<K, V> clock = null;
		clock = logClocks.get(host);
		if (clock == null) {
			clock = new TimeoutMapClock<K, V>(storage, host);
			TimeoutMapClock<K, V> oldClock = logClocks.putIfAbsent(host, clock);
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
		TimeoutMapClock<K, V> clock = ensureEventClock(host);
		clock.setTime(now, false);
	}

	@Override
	public List<V> expireQueue(String host) {
		TimeoutMapClock<K, V> clock = ensureEventClock(host);
		if (clock == null)
			return null;

		EvictQueue queue = clock.expireQueue();
		List<V> list = new ArrayList<V>(queue.size());
		for (int i = 0; i < queue.size(); i++) {
			long address = queue.get(i).getAddress();
			Entry<K, V> entry = storage.getEntry(address);
			if (entry != null)
				list.add(entry.getValue());
		}
		return list;
	}

	@Override
	public List<V> timeoutQueue(String host) {
		TimeoutMapClock<K, V> clock = ensureEventClock(host);
		if (clock == null)
			return null;

		EvictQueue queue = clock.timeoutQueue();
		List<V> list = new ArrayList<V>(queue.size());
		for (int i = 0; i < queue.size(); i++) {
			long address = queue.get(i).getAddress();
			Entry<K, V> entry = storage.getEntry(address);
			if (entry != null) {
				list.add(entry.getValue());
			}
		}
		return list;
	}

	@Override
	public long getLastTime(String host) {
		TimeoutMapClock<K, V> clock = ensureEventClock(host);
		return clock.getDate().getTime();
	}

	@Override
	public Set<String> hostSet() {
		return logClocks.keySet();
	}

	@Override
	public void addListener(TimeoutEventListener<K, V> listener) {
		storage.addListener(listener);
	}

	@Override
	public void removeListener(TimeoutEventListener<K, V> listener) {
		storage.removeListner(listener);
	}

	@Override
	public void clearClock() {
		realClock.clear();
		for (TimeoutMapClock<?, ?> clock : logClocks.values())
			clock.clear();
		
		logClocks.clear();
	}
}