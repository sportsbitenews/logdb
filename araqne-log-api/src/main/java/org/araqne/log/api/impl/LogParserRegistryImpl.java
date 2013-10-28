/*
 * Copyright 2013 Eediom Inc.
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
package org.araqne.log.api.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.confdb.Config;
import org.araqne.confdb.ConfigDatabase;
import org.araqne.confdb.ConfigService;
import org.araqne.confdb.Predicates;
import org.araqne.log.api.LogParser;
import org.araqne.log.api.LogParserEventListener;
import org.araqne.log.api.LogParserFactory;
import org.araqne.log.api.LogParserFactoryRegistry;
import org.araqne.log.api.LogParserProfile;
import org.araqne.log.api.LogParserRegistry;

@Component(name = "log-parser-registry")
@Provides
public class LogParserRegistryImpl implements LogParserRegistry {
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(LogParserRegistryImpl.class);

	@Requires
	private ConfigService conf;

	@Requires
	private LogParserFactoryRegistry parserFactoryRegistry;

	private ConcurrentMap<String, LogParserProfile> profiles;
	private CopyOnWriteArraySet<LogParserEventListener> listeners;

	@Validate
	public void start() {
		profiles = new ConcurrentHashMap<String, LogParserProfile>();

		ConfigDatabase db = conf.ensureDatabase("araqne-log-api");
		for (LogParserProfile p : db.find(LogParserProfile.class, null).getDocuments(LogParserProfile.class)) {
			profiles.put(p.getName(), p);
		}
		listeners = new CopyOnWriteArraySet<LogParserEventListener>();
	}

	@Invalidate
	public void stop() {
		profiles.clear();
	}

	@Override
	public List<LogParserProfile> getProfiles() {
		return new ArrayList<LogParserProfile>(profiles.values());
	}

	@Override
	public LogParserProfile getProfile(String name) {
		if (name == null)
			return null;
		return profiles.get(name);
	}

	@Override
	public void createProfile(LogParserProfile profile) {
		LogParserProfile old = profiles.putIfAbsent(profile.getName(), profile);
		if (old != null)
			throw new IllegalStateException("duplicated parser profile: " + profile.getName());

		ConfigDatabase db = conf.ensureDatabase("araqne-log-api");
		db.add(profile);

		for (LogParserEventListener listener : listeners) {
			try {
				listener.parserCreated(profile);
			} catch (Throwable t) {
				slog.warn("araqne log api: parser event listener should not throw any exception", t);
			}
		}
	}

	@Override
	public void removeProfile(String name) {
		ConfigDatabase db = conf.ensureDatabase("araqne-log-api");
		Config c = db.findOne(LogParserProfile.class, Predicates.field("name", name));
		if (c != null)
			c.remove();

		LogParserProfile old = profiles.remove(name);
		if (old == null)
			throw new IllegalStateException("parser profile not found: " + name);

		for (LogParserEventListener listener : listeners) {
			try {
				listener.parserRemoved(old);
			} catch (Throwable t) {
				slog.warn("araqne log api: parser event listener should not throw any exception", t);
			}
		}
	}

	@Override
	public LogParser newParser(String name) {
		if (name == null)
			throw new IllegalArgumentException("name should not be null");

		LogParserProfile profile = profiles.get(name);
		if (profile == null)
			throw new IllegalStateException("parser profile not found: " + name);

		LogParserFactory factory = parserFactoryRegistry.get(profile.getFactoryName());
		if (factory == null)
			throw new IllegalStateException("parser factory not found: " + profile.getFactoryName());

		return factory.createParser(profile.getConfigs());
	}

	public void addListener(LogParserEventListener listener) {
		if (listener == null)
			throw new IllegalArgumentException("listener should not be null");

		listeners.add(listener);
	}

	public void removeListener(LogParserEventListener listener) {
		if (listener == null)
			throw new IllegalArgumentException("listener should not be null");

		listeners.remove(listener);
	}
}
