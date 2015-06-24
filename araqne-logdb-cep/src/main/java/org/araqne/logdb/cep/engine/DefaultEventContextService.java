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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logdb.cep.Event;
import org.araqne.logdb.cep.EventContextService;
import org.araqne.logdb.cep.EventContextStorage;
import org.araqne.logdb.cep.EventSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name = "event-ctx-service")
@Provides(specifications = { EventContextService.class })
public class DefaultEventContextService implements EventContextService, EventSubscriber {
	private final Logger slog = LoggerFactory.getLogger(DefaultEventContextService.class);

	private ConcurrentHashMap<String, EventContextStorage> storages = new ConcurrentHashMap<String, EventContextStorage>();

	// topic to subscribers
	private ConcurrentHashMap<String, CopyOnWriteArraySet<EventSubscriber>> subscribers;

	@Validate
	public void start() {
		subscribers = new ConcurrentHashMap<String, CopyOnWriteArraySet<EventSubscriber>>();
	}

	@Invalidate
	public void stop() {
		subscribers.clear();
	}

	@Override
	public List<EventContextStorage> getStorages() {
		return new ArrayList<EventContextStorage>(storages.values());
	}

	@Override
	public EventContextStorage getStorage(String name) {
		if(name == null || name.equalsIgnoreCase("mem"))
			return storages.get("mem");
		else if(name.equalsIgnoreCase("redis"))
			return storages.get("redis");
		else
			return storages.get("mem");
	}

	@Override
	public void registerStorage(EventContextStorage storage) {
		EventContextStorage old = storages.putIfAbsent(storage.getName(), storage);
		if (old != null)
			throw new IllegalStateException("duplicated event context storage: " + storage.getName());

		storage.addSubscriber("*", this);
	}

	@Override
	public void unregisterStorage(EventContextStorage storage) {
		storages.remove(storage.getName(), storage);
		storage.removeSubscriber("*", this);
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

	@Override
	public void onEvent(Event ev) {
		CopyOnWriteArraySet<EventSubscriber> s = subscribers.get(ev.getKey().getTopic());
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
