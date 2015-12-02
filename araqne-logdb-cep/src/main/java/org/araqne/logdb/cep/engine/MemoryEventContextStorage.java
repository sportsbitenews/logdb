/*
 * Copyright 2014 Eediom Inc.
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

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
import org.araqne.logdb.cep.EventClockCallback;
import org.araqne.logdb.cep.EventClockItem;
import org.araqne.logdb.cep.EventContext;
import org.araqne.logdb.cep.EventContextListener;
import org.araqne.logdb.cep.EventContextService;
import org.araqne.logdb.cep.EventContextStorage;
import org.araqne.logdb.cep.EventKey;
import org.araqne.logdb.cep.EventSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name = "mem-event-ctx-storage")
public class MemoryEventContextStorage implements EventContextStorage, EventContextListener {
	private static final int INITIAL_CAPACITY = 11;

	private final Logger slog = LoggerFactory.getLogger(MemoryEventContextStorage.class);

	@Requires
	private EventContextService eventContextService;

	@Requires
	private TickService tickService;

	private ConcurrentHashMap<EventKey, EventContext> contexts;

	// topic to subscribers
	private ConcurrentHashMap<String, CopyOnWriteArraySet<EventSubscriber>> subscribers;

	// log tick host to aging context mappings
	private ConcurrentHashMap<String, EventClock<EventContext>> logClocks;
	private EventClock<EventContext> realClock;

	private RealClockTask realClockTask = new RealClockTask();

	@Override
	public String getName() {
		return "mem";
	}

	@Validate
	public void start() {
		contexts = new ConcurrentHashMap<EventKey, EventContext>();
		subscribers = new ConcurrentHashMap<String, CopyOnWriteArraySet<EventSubscriber>>();
		logClocks = new ConcurrentHashMap<String, EventClock<EventContext>>();
		realClock = new EventClock<EventContext>(new MemEventClockCallback(), "real", System.currentTimeMillis(), 10000);

		tickService.addTimer(realClockTask);
		eventContextService.registerStorage(this);
	}

	@Invalidate
	public void stop() {
		if (eventContextService != null) {
			eventContextService.unregisterStorage(this);
		}

		tickService.removeTimer(realClockTask);
		contexts.clear();
		subscribers.clear();
	}

	@Override
	public Set<String> getHosts() {
		return new HashSet<String>(logClocks.keySet());
	}

	@Override
	public EventClock<EventContext> getClock(String host) {
		return logClocks.get(host);
	}

	@Override
	public Iterator<EventKey> getContextKeys() {
		return new HashSet<EventKey>(contexts.keySet()).iterator();
	}

	@Override
	public Iterator<EventKey> getContextKeys(String topic) {
		if (topic == null)
			return new HashSet<EventKey>(contexts.keySet()).iterator();

		HashSet<EventKey> keys = new HashSet<EventKey>();

		for (EventKey key : contexts.keySet()) {
			if (key.getTopic().equals(topic))
				keys.add(key);
		}

		return keys.iterator();
	}

	@Override
	public EventContext getContext(EventKey key) {
		return contexts.get(key);
	}

	private EventClock<EventContext> ensureClock(ConcurrentHashMap<String, EventClock<EventContext>> clocks, String host,
			long time) {
		EventClock<EventContext> clock = null;
		clock = clocks.get(host);
		if (clock == null) {
			clock = new EventClock<EventContext>(new MemEventClockCallback(), host, time, INITIAL_CAPACITY);
			EventClock<EventContext> old = clocks.putIfAbsent(host, clock);
			if (old != null)
				return old;
			return clock;
		} else {
			return clock;
		}
	}

	@Override
	public void storeContext(EventContext ctx) {
		EventContext oldCtx = contexts.putIfAbsent(ctx.getKey(), ctx);
		if (oldCtx == null) {
			ctx.getListeners().add(this);

			if (ctx.getHost() != null) {
				EventClock<EventContext> logClock = ensureClock(logClocks, ctx.getHost(), ctx.getCreated());
				logClock.add(ctx);
			} else {
				realClock.add(ctx);
			}
			generateEvent(ctx, EventCause.CREATE);

		} else {
			if (ctx.getRows().size() > 0)
				oldCtx.addRow(ctx.getRows().get(0));
			oldCtx.getCounter().incrementAndGet();
			oldCtx.setTimeoutTime(ctx.getTimeoutTime());
		}
	}

	@Override
	public boolean addContextVariable(EventKey evtKey, String key, Object value) {
		EventContext ctx = getContext(evtKey);
		if (ctx == null)
			return false;

		ctx.setVariable(key, value);
		return true;
	}

	@Override
	public void storeContexts(List<EventContext> contexts) {
		for (EventContext context : contexts)
			storeContext(context);
	}

	@Override
	public void removeContext(EventKey key, EventCause cause) {
		EventContext ctx = getContext(key);

		if (contexts.remove(key, ctx)) {
			ctx.getListeners().remove(this);

			if (key.getHost() != null) {
				EventClock<EventContext> logClock = ensureClock(logClocks, ctx.getHost(), ctx.getCreated());
				logClock.remove(ctx);
			} else {
				realClock.remove(ctx);
			}

			generateEvent(ctx, cause);
		}
	}

	@Override
	public void advanceTime(String host, long logTime) {
		EventClock<EventContext> logClock = ensureClock(logClocks, host, logTime);
		logClock.setTime(logTime, false);
	}

	@Override
	public void clearClocks() {
		contexts.clear();
		realClock = new EventClock<EventContext>(new MemEventClockCallback(), "real", System.currentTimeMillis(), 10000);
		logClocks = new ConcurrentHashMap<String, EventClock<EventContext>>();
	}

	@Override
	public void clearContexts() {
		clearContexts(null);
	}

	@Override
	public void clearContexts(String topic) {
		Iterator<EventKey> itr = getContextKeys();

		while (itr.hasNext()) {
			EventKey key = itr.next();
			if (topic == null || key.getTopic().equals(topic)) {
				removeContext(key, EventCause.REMOVAL);
			}
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
					realClock.setTime(System.currentTimeMillis(), false);
				} finally {
					running.set(false);
				}
			}
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

	@Override
	public void onUpdateTimeout(EventContext ctx) {
		// update real clock queue
		if (ctx.getHost() == null) {
			realClock.updateTimeout(ctx);
			return;
		}

		// update per-host case only
		EventClock<EventContext> clock = ensureClock(logClocks, ctx.getHost(), ctx.getCreated());
		if (clock == null)
			return;

		clock.updateTimeout(ctx);
	}

	@Override
	public void removeContexts(List<EventKey> contexts, EventCause removal) {
		for (EventKey key : contexts) {
			removeContext(key, removal);
		}
	}

	@Override
	public EventContext[] getContexts(List<EventKey> keys) {
		EventContext[] contexts = new EventContext[keys.size()];

		for (int i = 0; i < keys.size(); i++) {
			if (keys.get(i) != null)
				contexts[i] = getContext(keys.get(i));
		}

		return contexts;
	}

	@Override
	public Object[] addContextVariables(List<Map<String, Object>> contexts) {
		Object[] ret = new Object[contexts.size()];

		for (int i = 0; i < contexts.size(); i++) {
			Map<String, Object> map = contexts.get(i);
			if (map == null) {
				ret[i] = false;
				continue;
			}

			EventKey evtKey = (EventKey) map.get("evtkey");
			String key = (String) map.get("key");
			Object value = map.get("value");
			ret[i] = addContextVariable(evtKey, key, value);
		}

		return ret;
	}

	private class MemEventClockCallback implements EventClockCallback {

		@Override
		public void onRemove(EventClockItem value, EventCause expire) {
			removeContext(value.getKey(), expire);
		}
	}

}
