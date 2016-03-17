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
package org.araqne.logdb.cep.engine;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.cron.AbstractTickTimer;
import org.araqne.cron.TickService;
import org.araqne.logdb.cep.Event;
import org.araqne.logdb.cep.EventCause;
import org.araqne.logdb.cep.EventClock;
import org.araqne.logdb.cep.EventContext;
import org.araqne.logdb.cep.EventContextListener;
import org.araqne.logdb.cep.EventContextService;
import org.araqne.logdb.cep.EventContextStorage;
import org.araqne.logdb.cep.EventKey;
import org.araqne.logdb.cep.EventSubscriber;
import org.araqne.logdb.cep.offheap.ConcurrentOffHeapHashMap;
import org.araqne.logdb.cep.offheap.engine.ReferenceStorageEngineFactory;
import org.araqne.logdb.cep.offheap.engine.StorageEngineFactory;
import org.araqne.logdb.cep.offheap.engine.serialize.EventContextSerialize;
import org.araqne.logdb.cep.offheap.engine.serialize.EventKeySerialize;
import org.araqne.logdb.cep.offheap.timeout.OffHeapEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name = "offheap-event-ctx-storage")
public class OffHeapEventContextStorage implements EventContextStorage, EventContextListener {
	private final Logger slog = LoggerFactory.getLogger(OffHeapEventContextStorage.class);
	// private static final int INITIAL_CAPACITY = 11;

	@Requires
	private EventContextService eventContextService;

	@Requires
	private TickService tickService;

	// private ConcurrentHashMap<String, ConcurrentOffHeapHashMap<EventKey,
	// EventContext>> contextsMap;
	// private EventClock<EventContext> clock;

	// topic to subscribers
	private ConcurrentHashMap<String, CopyOnWriteArraySet<EventSubscriber>> subscribers;

	// log tick host to aging context mappings
	// private ConcurrentHashMap<String, EventClock<EventContext>> logClocks;
	// private EventClock<EventContext> realClock;

	private ConcurrentOffHeapHashMap<EventKey, EventContext> contexts;// Map;
	private RealClockTask realClockTask = new RealClockTask();
	private CepListener listener = new CepListener();

	// private OffheapEventClock clocks = new OffheapEventClock();

	@Override
	public String getName() {
		return "offheap";
	}

	@Validate
	public void start() {
		StorageEngineFactory<EventKey, EventContext> factory = new ReferenceStorageEngineFactory<EventKey, EventContext>(
				1024 * 1024 * 16, Integer.MAX_VALUE >> 5, new EventKeySerialize(), new EventContextSerialize());
		contexts = new ConcurrentOffHeapHashMap<EventKey, EventContext>(factory);
		contexts.addListener(listener);
		subscribers = new ConcurrentHashMap<String, CopyOnWriteArraySet<EventSubscriber>>();
		tickService.addTimer(realClockTask);
		eventContextService.registerStorage(this);

	}

	@Invalidate
	public void stop() {
		if (eventContextService != null) {
			eventContextService.unregisterStorage(this);
		}

		tickService.removeTimer(realClockTask);
		contexts.close();
		subscribers.clear();
	}

	// ctx 저장 -> 기존에 존재하면 row만 추가, 없으면 ctx전체 추가
	// 기존 ctx를 가져온다
	// ctx add
	// 기존 변경된 값이 변화되지 않으면 input
	// 변경되었으면 첨부터 다시..
	@Override
	public void storeContext(EventContext ctx) {
		while (true) {
			EventContext oldCtx = contexts.get(ctx.getKey());
			if (oldCtx == null) { /* 신규 추가 */
				if (contexts.putIfAbsent(ctx.getKey(), ctx, ctx.getHost(),
						getMinValue(ctx.getTimeoutTime(), ctx.getExpireTime())) == null) {
					if (ctx.getTimeoutTime() > 0
							&& getMinValue(ctx.getTimeoutTime(), ctx.getExpireTime()) == ctx.getTimeoutTime()) {
						contexts.timeout(ctx.getKey(), ctx.getHost(), ctx.getTimeoutTime());
					}
					generateEvent(ctx, EventCause.CREATE);
					break;
				}
			} else {
				EventContext newCtx = oldCtx.clone();
				if (ctx.getRows().size() > 0)
					newCtx.addRow(ctx.getRows().get(0));

				newCtx.getCounter().incrementAndGet();
				newCtx.setTimeoutTime(ctx.getTimeoutTime());
				if (contexts.replace(ctx.getKey(), oldCtx, newCtx, ctx.getHost(), ctx.getTimeoutTime())) {
					break;
				}
			}
		}
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
	public Set<String> getHosts() {
		return contexts.hostSet();
	}

	@Override
	public EventClock<EventContext> getClock(String host) {
		return new OffheapEventClock(host);
	}

	@Override
	public Iterator<EventKey> getContextKeys() {
		return contexts.getKeys();
	}

	@Override
	public Iterator<EventKey> getContextKeys(String topic) {
		if (topic == null)
			return contexts.getKeys();

		return new keyIterator(topic);
	}

	@Override
	public EventContext getContext(EventKey key) {
		return contexts.get(key);
	}

	@Override
	public void addContextVariable(EventKey eventKey, String key, Object value) {
		while (true) {
			EventContext ctx = contexts.get(eventKey);
			if (ctx == null)
				return;

			EventContext oldCtx = ctx.clone();
			ctx.setVariable(key, value);
			if (contexts.replace(eventKey, oldCtx, ctx))
				break;
		}
	}

	@Override
	public void storeContexts(List<EventContext> contexts) {
		for (EventContext context : contexts)
			storeContext(context);
	}

	@Override
	public void removeContext(EventKey key, EventCause cause) {
		EventContext ctx = contexts.get(key);

		if (contexts.remove(key) != null) {
			generateEvent(ctx, cause);
		}
	}

	@Override
	public void advanceTime(String host, long logTime) {
		contexts.setTime(host, logTime);
	}

	@Override
	public void clearClocks() {
		contexts.clearClock();
	}

	@Override
	public void clearContexts() {
		clearContexts(null);
	}

	@Override
	public void clearContexts(String topic) {
		Iterator<EventKey> itr = getContextKeys(topic);

		while (itr.hasNext()) {
			EventKey key = itr.next();
			removeContext(key, EventCause.REMOVAL);
			// if (topic == null || key.getTopic().equals(topic)) {
			// }
		}
	}

	@Override
	public void addSubscriber(String topic, EventSubscriber subscriber) {
		CopyOnWriteArraySet<EventSubscriber> s = new CopyOnWriteArraySet<EventSubscriber>();
		CopyOnWriteArraySet<EventSubscriber> old = subscribers.putIfAbsent(topic, s);
		if (old != null)
			s = old;

		s.add(subscriber);
	}

	@Override
	public void removeSubscriber(String topic, EventSubscriber subscriber) {
		CopyOnWriteArraySet<EventSubscriber> s = subscribers.get(topic);
		if (s != null)
			s.remove(subscriber);
	}

	private class RealClockTask extends AbstractTickTimer {
		private AtomicBoolean running = new AtomicBoolean();

		@Override
		public int getInterval() {
			return 100;
		}

		@Override
		public void onTick() {
			if (running.compareAndSet(false, true)) {
				try {
					contexts.setTime(null, System.currentTimeMillis());
				} finally {
					running.set(false);
				}
			}
		}
	}

	@Override
	public void onUpdateTimeout(EventContext ctx) {
		// update real clock queue
		contexts.timeout(ctx.getKey(), ctx.getHost(), ctx.getTimeoutTime());
		// // if (ctx.getHost() == null) {
		// // realClock.updateTimeout(ctx);
		// // return;
		// // }
		//
		// // update per-host case only
		// // EventClock<EventContext> clock = ensureClock(logClocks,
		// // ctx.getHost(), ctx.getCreated());
		// // if (clock == null)
		// // return;
		// // clock.updateTimeout(ctx);
	}

	@Override
	public void removeContexts(List<EventKey> contexts, EventCause removal) {
		for (EventKey key : contexts) {
			removeContext(key, removal);
		}
	}

	@Override
	public List<EventContext> getContexts(Set<EventKey> keys) {
		List<EventContext> contexts = new ArrayList<EventContext>();
		for (EventKey key : keys) {
			contexts.add(getContext(key));
		}
		return contexts;
	}

	private class CepListener implements OffHeapEventListener<EventKey, EventContext> {

		@Override
		public void onExpire(EventKey key, EventContext ctx, long time) {
			if (ctx.getExpireTime() == time)
				generateEvent(ctx, EventCause.EXPIRE);
			else
				generateEvent(ctx, EventCause.TIMEOUT);
		}
	}

	private void generateEvent(EventContext ctx, EventCause cause) {
		if (slog.isDebugEnabled())
			slog.debug("araqne logdb cep: generate event ctx [{}] cause [{}]", ctx.getKey(), cause);

		Event ev = new Event(ctx, cause);
		ev.getRows().addAll(ctx.getRows());

		CopyOnWriteArraySet<EventSubscriber> s = subscribers.get(ctx.getKey().getTopic());
		if (s != null) {
			for (EventSubscriber subscriber : s) {
				try {
					subscriber.onEvent(ev);
				} catch (Throwable t) {
					slog.error("araqne logdb cep: subscriber should not throw any exception", t);
				}
			}
		}

		// for wild subscriber
		s = subscribers.get("*");
		if (s != null) {
			for (EventSubscriber subscriber : s) {
				try {
					subscriber.onEvent(ev);
				} catch (Throwable t) {
					slog.error("araqne logdb cep: subscriber should not throw any exception", t);
				}
			}
		}
	}

	private class OffheapEventClock extends EventClock<EventContext> {
		private static final int INITIAL_CAPACITY = 11;

		private String host;

		public OffheapEventClock(String host) {
			super(null, null, 0, INITIAL_CAPACITY);
			this.host = host;
		}

		@Override
		public List<EventContext> getTimeoutContexts() {
			return contexts.timeoutQueue(host);
		}

		@Override
		public List<EventContext> getExpireContexts() {
			return contexts.expireQueue(host);
		}

		@Override
		public String toString() {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
			return host + " (timeout: " + contexts.timeoutQueue(host).size() + ", expire: "
					+ contexts.expireQueue(host).size() + ") => " + df.format(new Date(contexts.getLastTime(host)));
		}

	}

	private class keyIterator implements Iterator<EventKey> {
		String filter;
		Iterator<EventKey> contextItr = contexts.getKeys();
		int size = 5000;
		Set<EventKey> keys;
		Iterator<EventKey> keyItr;
		boolean lastScan = false;

		public keyIterator(String filter) {
			this.filter = filter;
			keys = new HashSet<EventKey>(size);
			scan();
		}

		public void scan() {
			keys.clear();
			while (keys.size() <= size) {
				if (!contextItr.hasNext()) {
					lastScan = true;
					break;
				}

				EventKey key = contextItr.next();
				String topic = key.getTopic();
				if (topic != null && topic.equals(filter)) {
					keys.add(key);
				}
			}
			keyItr = keys.iterator();
		}

		@Override
		public boolean hasNext() {
			if (!keyItr.hasNext() && !lastScan) {
				scan();
			}
			return keyItr.hasNext();
		}

		@Override
		public EventKey next() {
			if (!keyItr.hasNext() && !lastScan) {
				scan();
			}
			return keyItr.next();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
}

// private class MemEventClockCallback implements EventClockCallback {
//
// @Override
// public void onRemove(EventClockItem value, EventCause expire) {
// removeContext(value.getKey(), expire);
// }
// }

// private ConcurrentOffHeapHashMap<EventKey, EventContext>
// ensureContexts(String host) {
// ConcurrentOffHeapHashMap<EventKey, EventContext> contexts = null;
// contexts = contextsMap.get(host);
// if (contexts == null) {
// StorageEngine<EventKey, EventContext> storage = new
// ReferenceStorageEngine<EventKey, EventContext>(
// new EventKeySerialize(), new EventContextSerialize());
// contexts = new ConcurrentOffHeapHashMap<EventKey, EventContext>(storage);
// ConcurrentOffHeapHashMap<EventKey, EventContext> old =
// contextsMap.putIfAbsent(host, contexts);
// if (old != null)
// return old;
// return contexts;
// } else {
// return contexts;
// }
// }

// private EventClock<EventContext> ensureClock(ConcurrentHashMap<String,
// EventClock<EventContext>> clocks, String host,
// long time) {
// EventClock<EventContext> clock = null;
// clock = clocks.get(host);
// if (clock == null) {
// clock = new EventClock<EventContext>(new MemEventClockCallback(), host,
// time, INITIAL_CAPACITY);
// EventClock<EventContext> old = clocks.putIfAbsent(host, clock);
// if (old != null)
// return old;
// return clock;
// } else {
// return clock;
// }
// }

// private StorageEngine<K, V> engine;
// private AtomicLong lastTime = new AtomicLong();
// private String host;
// private TimeoutQueue timeoutQueue;
// private Un_ExpireQueue expireQueue;
//
// private final TimeoutComparator timeoutComparator = new
// TimeoutComparator();
// private final ExpireComparator expireComparator = new
// ExpireComparator();
// private final TimeoutUnitComparator comparator = new
// TimeoutUnitComparator();
//
// public OffHeapEventClock(StorageEngine<K, V> engine,
// EventClockCallback callback, String host, long lastTime,
// int initialCapacity) {
// super(callback, host, lastTime, initialCapacity);
// this.engine = engine;
// this.host = host;
// this.lastTime = new AtomicLong(lastTime);
// }
//
// public OffHeapEventClock(StorageEngine<K, V> engine, String host,
// long lastTime) {
// this(engine, null, host, lastTime, INITIAL_CAPACITY);
// }
//
// public OffHeapEventClock(StorageEngine<K, V> engine, String host) {
// this(engine, null, host, new Date().getTime(), INITIAL_CAPACITY);
// }
//
// // thread unsafe -> 상위 단계인 offheapmap에서 lock
// public void addExpireTime(TimeoutItem item) {
// expireQueue.add(item);
// }
//
// // thread unsafe -> 상위 단계인 offheapmap에서 lock
// public void addTimeoutTime(TimeoutItem item) {
// timeoutQueue.add(item);
// }
//
// public TimeoutQueue timeoutQueue() {
// return timeoutQueue;
// }
//
// public Un_ExpireQueue expireQueue() {
// return expireQueue;
// }
//
// @Override
// public String getHost() {
// return host;
// }
//
// @Override
// public Date getTime() {
// return new Date(lastTime.get());
// }
//
// @Override
// public List<K> getTimeoutContexts() {
// List<K> l = new ArrayList<K>();
// Collections.sort(l, comparator);
// return l;
// }
//
// @Override
// public List<K> getExpireContexts() {
// List<K> l = new ArrayList<K>(expireQueue.size());
//
// for (int i = 0; i < expireQueue.size(); i++) {
// TimeoutItem item = expireQueue.get(i);
// K key = engine.loadKey(item.getAddress());
// l.add(key);
// }
//
// Collections.sort(l, comparator);
// return l;
// }
//
// @Override
// public int getTimeoutQueueLength() {
// return getTimeoutContexts().size();
// }
//
// @Override
// public int getExpireQueueLength() {
// return getExpireContexts().size();
// }
//
// @Override
// public void setTime(long now, boolean force) {
// throw new UnsupportedOperationException();
// }
//
// @Override
// public void add(K item) {
// throw new UnsupportedOperationException();
// }
//
// @Override
// public void updateTimeout(K item) {
// throw new UnsupportedOperationException();
// }
//
// @Override
// public void remove(K item) {
// throw new UnsupportedOperationException();
// }
//
// @Override
// public String toString() {
// SimpleDateFormat df = new
// SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
// return host + " (timeout: " + getTimeoutQueueLength() + ", expire: "
// + getExpireQueueLength() + ") => "
// + df.format(new Date(lastTime.get()));
// }
//
// private class ExpireComparator implements Comparator<K> {
//
// @Override
// public int compare(K o1, K o2) {
// return 0;
// }
//
// }
//
// private class TimeoutUnitComparator implements
// Comparator<TimeoutItem> {
// @Override
// public int compare(TimeoutItem o1, TimeoutItem o2) {
// long t1 = o1.getDate();
// long t2 = o2.getDate();
//
// if (t1 == t2)
// return 0;
// return t1 < t2 ? -1 : 1;
//
//