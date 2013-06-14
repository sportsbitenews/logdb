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
import org.araqne.log.api.LogNormalizer;
import org.araqne.log.api.LogNormalizerFactory;
import org.araqne.log.api.LogNormalizerFactoryRegistry;
import org.araqne.log.api.LogNormalizerProfile;
import org.araqne.log.api.LogNormalizerRegistry;

@Component(name = "log-normalizer-registry")
@Provides
public class LogNormalizerRegistryImpl implements LogNormalizerRegistry {
	@Requires
	private ConfigService conf;

	@Requires
	private LogNormalizerFactoryRegistry factoryRegistry;

	private ConcurrentMap<String, LogNormalizerProfile> profiles;

	@Validate
	public void start() {
		profiles = new ConcurrentHashMap<String, LogNormalizerProfile>();

		ConfigDatabase db = conf.ensureDatabase("araqne-log-api");
		ConfigIterator it = db.find(LogNormalizerProfile.class, null);
		for (LogNormalizerProfile p : it.getDocuments(LogNormalizerProfile.class)) {
			profiles.put(p.getName(), p);
		}
	}

	@Invalidate
	public void stop() {
		profiles.clear();
	}

	@Override
	public List<LogNormalizerProfile> getProfiles() {
		return new ArrayList<LogNormalizerProfile>(profiles.values());
	}

	@Override
	public LogNormalizerProfile getProfile(String name) {
		if (name == null)
			return null;
		return profiles.get(name);
	}

	@Override
	public void createProfile(LogNormalizerProfile profile) {
		LogNormalizerProfile old = profiles.putIfAbsent(profile.getName(), profile);
		if (old != null)
			throw new IllegalStateException("duplicated normalizer profile: " + profile.getName());

		ConfigDatabase db = conf.ensureDatabase("araqne-log-api");
		db.add(profile);
	}

	@Override
	public void removeProfile(String name) {
		ConfigDatabase db = conf.ensureDatabase("araqne-log-api");
		Config c = db.findOne(LogNormalizerProfile.class, Predicates.field("name", name));
		if (c != null)
			c.remove();

		LogNormalizerProfile old = profiles.remove(name);
		if (old == null)
			throw new IllegalStateException("normalizer profile not found: " + name);
	}

	@Override
	public LogNormalizer newNormalizer(String name) {
		LogNormalizerProfile profile = profiles.get(name);
		if (profile == null)
			throw new IllegalStateException("normalizer profile not found: " + name);

		LogNormalizerFactory factory = factoryRegistry.get(profile.getFactoryName());
		if (factory == null)
			throw new IllegalStateException("normalizer factory not found: " + profile.getFactoryName());

		return factory.createNormalizer(profile.getConfigs());
	}
}
