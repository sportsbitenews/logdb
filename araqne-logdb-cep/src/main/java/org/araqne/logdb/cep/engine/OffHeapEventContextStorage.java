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
import org.araqne.logdb.cep.offheap.TimeoutMap;
import org.araqne.logdb.cep.offheap.evict.TimeoutEventListener;
import org.araqne.logdb.cep.offheap.factory.ConcurrentTimeoutMapFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name = "offheap-event-ctx-storage")
public class OffHeapEventContextStorage implements EventContextStorage, EventContextListener {
	private final Logger slog = LoggerFactory.getLogger(OffHeapEventContextStorage.class);

	@Requires
	private EventContextService eventContextService;

	@Requires
	private TickService tickService;

	private ConcurrentHashMap<String, CopyOnWriteArraySet<EventSubscriber>> subscribers;
	private TimeoutMap<EventKey, EventContext> contexts;
	private RealClockTask realClockTask = new RealClockTask();
	private CepListener listener = new CepListener();

	@Override
	public String getName() {
		return "offheap";
	}

	@Validate
	public void start() {
		contexts = ConcurrentTimeoutMapFactory.event(7, 1024 * 1024, 1024).map();
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

	@Override
	public void storeContext(EventContext ctx) {
		while (true) {
			EventContext oldCtx = contexts.get(ctx.getKey());
			if (oldCtx == null) {
				if (contexts.putIfAbsent(ctx.getKey(), ctx, ctx.getHost(), ctx.getExpireTime(), ctx.getTimeoutTime())) {
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

	@Override
	public Set<String> getHosts() {
		return contexts.hostSet();
	}

	@Override
	public EventClock<EventContext> getClock(String host) {
		if (!contexts.hostSet().contains(host))
			return null;

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
			if (contexts.replace(eventKey, oldCtx, ctx, null, 0L))
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

		if (contexts.remove(key)) {
			generateEvent(ctx, cause);
		}
	}

	@Override
	public void advanceTime(String host, long logTime) {
		contexts.setTime(host, logTime);
	}

	@Override
	public void clearClocks() {
		contexts.clear();
		// generate log
		// clearContexts();
		// contexts.clearClock();
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

	private class CepListener implements TimeoutEventListener<EventKey, EventContext> {

		@Override
		public void onTimeout(EventKey key, EventContext ctx, long time) {
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
		public Date getTime() {
			return new Date(contexts.getLastTime(host));
		}

		@Override
		public int getExpireQueueLength() {
			return contexts.expireQueue(host).size();
		}

		@Override
		public int getTimeoutQueueLength() {
			return contexts.timeoutQueue(host).size();
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
