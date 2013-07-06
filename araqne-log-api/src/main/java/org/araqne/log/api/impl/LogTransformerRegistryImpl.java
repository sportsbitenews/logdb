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
import org.araqne.confdb.ConfigIterator;
import org.araqne.confdb.ConfigService;
import org.araqne.confdb.Predicates;
import org.araqne.log.api.LogTransformer;
import org.araqne.log.api.LogTransformerFactory;
import org.araqne.log.api.LogTransformerFactoryRegistry;
import org.araqne.log.api.LogTransformerFactoryRegistryEventListener;
import org.araqne.log.api.LogTransformerProfile;
import org.araqne.log.api.LogTransformerRegistry;
import org.araqne.log.api.LogTransformerRegistryEventListener;
import org.araqne.log.api.Logger;
import org.araqne.log.api.LoggerRegistry;

@Component(name = "log-transformer-registry")
@Provides
public class LogTransformerRegistryImpl implements LogTransformerRegistry {
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(LogTransformerRegistryImpl.class);

	@Requires
	private ConfigService conf;

	@Requires
	private LogTransformerFactoryRegistry factoryRegistry;

	@Requires
	private LoggerRegistry loggerRegistry;

	private ConcurrentMap<String, ProfileStatus> profileStatuses;

	private CopyOnWriteArraySet<LogTransformerRegistryEventListener> listeners;

	private ProfileUpdater updater = new ProfileUpdater();

	@Validate
	public void start() {
		listeners = new CopyOnWriteArraySet<LogTransformerRegistryEventListener>();
		profileStatuses = new ConcurrentHashMap<String, ProfileStatus>();

		ConfigDatabase db = conf.ensureDatabase("araqne-log-api");
		ConfigIterator it = db.find(LogTransformerProfile.class, null);

		for (LogTransformerProfile p : it.getDocuments(LogTransformerProfile.class)) {
			loadProfile(p);
		}

		factoryRegistry.addListener(updater);
	}

	@Invalidate
	public void stop() {
		if (factoryRegistry != null)
			factoryRegistry.removeListener(updater);

		listeners.clear();
		profileStatuses.clear();
	}

	@Override
	public List<LogTransformerProfile> getProfiles() {
		ArrayList<LogTransformerProfile> profiles = new ArrayList<LogTransformerProfile>();
		for (ProfileStatus status : profileStatuses.values()) {
			if (status.valid)
				profiles.add(status.profile);
		}

		return profiles;
	}

	@Override
	public LogTransformerProfile getProfile(String name) {
		if (name == null)
			return null;
		ProfileStatus s = profileStatuses.get(name);
		if (s == null)
			return null;
		return s.valid ? s.profile : null;
	}

	@Override
	public void createProfile(LogTransformerProfile profile) {
		loadProfile(profile);

		ConfigDatabase db = conf.ensureDatabase("araqne-log-api");
		db.add(profile);
	}

	@Override
	public void removeProfile(String name) {
		ConfigDatabase db = conf.ensureDatabase("araqne-log-api");
		Config c = db.findOne(LogTransformerProfile.class, Predicates.field("name", name));
		if (c != null)
			c.remove();

		unloadProfile(name);
	}

	private void loadProfile(LogTransformerProfile profile) {
		boolean valid = factoryRegistry.getFactory(profile.getFactoryName()) != null;
		ProfileStatus status = new ProfileStatus(profile, valid);
		ProfileStatus old = profileStatuses.putIfAbsent(profile.getName(), status);
		if (old != null)
			throw new IllegalStateException("duplicated transformer profile: " + profile.getName());

		setTransformers(profile);

		for (LogTransformerRegistryEventListener listener : listeners) {
			try {
				listener.profileAdded(profile);
			} catch (Throwable t) {
				slog.warn("araqne log api: transformer registry listener should not throw any exception", t);
			}
		}
	}

	private void setTransformers(LogTransformerProfile profile) {
		for (Logger logger : loggerRegistry.getLoggers()) {
			String transformerName = logger.getConfig().get("transformer");
			if (profile.getName().equals(transformerName)) {
				try {
					LogTransformer transformer = newTransformer(transformerName);
					logger.setTransformer(transformer);
				} catch (Throwable t) {
					slog.error("araqne log api: cannot start pending logger, " + logger.getFullName(), t);
				}
			}
		}
	}

	private void unloadProfile(String name) {
		ProfileStatus old = profileStatuses.remove(name);
		if (old == null)
			throw new IllegalStateException("transformer profile not found: " + name);

		// unset transformer and stop loggers
		unsetTransformers(name);

		for (LogTransformerRegistryEventListener listener : listeners) {
			try {
				listener.profileRemoved(old.profile);
			} catch (Throwable t) {
				slog.warn("araqne log api: transformer registry listener should not throw any exception", t);
			}
		}
	}

	private void unsetTransformers(String name) {
		for (Logger logger : loggerRegistry.getLoggers()) {
			String transformerName = logger.getConfig().get("transformer");
			if (transformerName != null && transformerName.equals(name)) {
				logger.setTransformer(null);
			}
		}
	}

	@Override
	public LogTransformer newTransformer(String name) {
		ProfileStatus status = profileStatuses.get(name);
		if (status == null)
			throw new IllegalStateException("transformer profile not found: " + name);

		LogTransformerProfile profile = status.profile;
		LogTransformerFactory factory = factoryRegistry.getFactory(profile.getFactoryName());
		if (factory == null)
			throw new IllegalStateException("transformer factory not found: " + profile.getFactoryName());

		return factory.newTransformer(profile.getConfigs());
	}

	@Override
	public void addListener(LogTransformerRegistryEventListener listener) {
		listeners.add(listener);
	}

	@Override
	public void removeListener(LogTransformerRegistryEventListener listener) {
		listeners.remove(listener);
	}

	private class ProfileUpdater implements LogTransformerFactoryRegistryEventListener {

		@Override
		public void factoryAdded(LogTransformerFactory factory) {
			slog.debug("araqne log api: transformer factory [{}] added", factory.getName());
			for (ProfileStatus s : profileStatuses.values()) {
				if (s.profile.getFactoryName().equals(factory.getName())) {
					slog.debug("araqne log api: validating transformer profile [{}]", s.profile.getName());
					s.valid = true;
					setTransformers(s.profile);
				}
			}
		}

		@Override
		public void factoryRemoved(LogTransformerFactory factory) {
			for (ProfileStatus s : profileStatuses.values()) {
				if (s.profile.getFactoryName().equals(factory.getName())) {
					slog.debug("araqne log api: invalidating transformer profile [{}]", s.profile.getName());
					s.valid = false;
					unsetTransformers(s.profile.getName());
				}
			}
		}
	}

	private static class ProfileStatus {
		public LogTransformerProfile profile;
		public boolean valid;

		public ProfileStatus(LogTransformerProfile profile, boolean valid) {
			this.profile = profile;
			this.valid = valid;
		}
	}
}
