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

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logdb.cep.Event;
import org.araqne.logdb.cep.EventCause;
import org.araqne.logdb.cep.EventContext;
import org.araqne.logdb.cep.EventContextService;
import org.araqne.logdb.cep.EventContextStorage;
import org.araqne.logdb.cep.EventKey;
import org.araqne.logdb.cep.EventSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name = "mem-event-ctx-storage")
public class MemoryEventContextStorage implements EventContextStorage {
	private final Logger slog = LoggerFactory.getLogger(MemoryEventContextStorage.class);

	@Requires
	private EventContextService eventContextService;

	private ConcurrentHashMap<EventKey, EventContext> contexts;

	// topic to subscribers
	private ConcurrentHashMap<String, CopyOnWriteArraySet<EventSubscriber>> subscribers;

	private TimeoutChecker timeoutChecker = new TimeoutChecker();
	private Timer timer;

	@Override
	public String getName() {
		return "mem";
	}

	@Validate
	public void start() {
		contexts = new ConcurrentHashMap<EventKey, EventContext>();
		subscribers = new ConcurrentHashMap<String, CopyOnWriteArraySet<EventSubscriber>>();
		timer = new Timer("Event Context Timeout Checker", true);
		timer.scheduleAtFixedRate(timeoutChecker, 0, 1000);

		eventContextService.registerStorage(this);
	}

	@Invalidate
	public void stop() {
		if (eventContextService != null) {
			eventContextService.unregisterStorage(this);
		}

		timeoutChecker.cancel();
		contexts.clear();
		subscribers.clear();
	}

	@Override
	public EventContext getContext(EventKey key) {
		return contexts.get(key);
	}

	@Override
	public EventContext addContext(EventContext ctx) {
		EventContext old = contexts.putIfAbsent(ctx.getKey(), ctx);
		if (old == null)
			return ctx;
		return old;
	}

	@Override
	public void removeContext(EventKey key) {
		EventContext ctx = contexts.remove(key);
		if (ctx != null)
			generateEvent(ctx, EventCause.REMOVAL);
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

	private class TimeoutChecker extends TimerTask {

		@Override
		public void run() {
			List<EventContext> evictExpire = new ArrayList<EventContext>();

			long now = System.currentTimeMillis();
			for (EventContext ctx : contexts.values()) {
				long expire = ctx.getExpireTime();
				if (expire != 0 && expire <= now)
					evictExpire.add(ctx);
			}

			for (EventContext ctx : evictExpire) {
				EventKey key = ctx.getKey();
				if (slog.isDebugEnabled())
					slog.debug("araqne logdb cep: generate timeout event, topic [{}] key [{}]", key.getTopic(), key.getKey());

				contexts.remove(key);
				generateEvent(ctx, EventCause.EXPIRE);
			}

			List<EventContext> evictTimeout = new ArrayList<EventContext>();

			for (EventContext ctx : contexts.values()) {
				long time = ctx.getTimeoutTime();
				if (time != 0 && time <= now)
					evictTimeout.add(ctx);
			}

			for (EventContext ctx : evictTimeout) {
				EventKey key = ctx.getKey();
				if (slog.isDebugEnabled())
					slog.debug("araqne logdb cep: generate timeout event, topic [{}] key [{}]", key.getTopic(), key.getKey());

				contexts.remove(key);
				generateEvent(ctx, EventCause.TIMEOUT);
			}
		}
	}

	private void generateEvent(EventContext ctx, EventCause cause) {
		Event ev = new Event(ctx.getKey(), cause);
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
}
