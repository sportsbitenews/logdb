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
import org.araqne.log.api.LogTransformer;
import org.araqne.log.api.LogTransformerFactory;
import org.araqne.log.api.LogTransformerFactoryRegistry;
import org.araqne.log.api.LogTransformerProfile;
import org.araqne.log.api.LogTransformerRegistry;

@Component(name = "log-transformer-registry")
@Provides
public class LogTransformerRegistryImpl implements LogTransformerRegistry {

	@Requires
	private ConfigService conf;

	@Requires
	private LogTransformerFactoryRegistry factoryRegistry;

	private ConcurrentMap<String, LogTransformerProfile> profiles;

	@Validate
	public void start() {
		profiles = new ConcurrentHashMap<String, LogTransformerProfile>();

		ConfigDatabase db = conf.ensureDatabase("araqne-log-api");
		ConfigIterator it = db.find(LogTransformerProfile.class, null);

		for (LogTransformerProfile p : it.getDocuments(LogTransformerProfile.class)) {
			profiles.put(p.getName(), p);
		}
	}

	@Invalidate
	public void stop() {
		profiles.clear();
	}

	@Override
	public List<LogTransformerProfile> getProfiles() {
		return new ArrayList<LogTransformerProfile>(profiles.values());
	}

	@Override
	public LogTransformerProfile getProfile(String name) {
		if (name == null)
			return null;
		return profiles.get(name);
	}

	@Override
	public void createProfile(LogTransformerProfile profile) {
		LogTransformerProfile old = profiles.putIfAbsent(profile.getName(), profile);
		if (old != null)
			throw new IllegalStateException("duplicated transformer profile: " + profile.getName());

		ConfigDatabase db = conf.ensureDatabase("araqne-log-api");
		db.add(profile);
	}

	@Override
	public void removeProfile(String name) {
		ConfigDatabase db = conf.ensureDatabase("araqne-log-api");
		Config c = db.findOne(LogTransformerProfile.class, Predicates.field("name", name));
		if (c != null)
			c.remove();

		LogTransformerProfile old = profiles.remove(name);
		if (old == null)
			throw new IllegalStateException("transformer profile not found: " + name);
	}

	@Override
	public LogTransformer newTransformer(String name) {
		LogTransformerProfile profile = profiles.get(name);
		if (profile == null)
			throw new IllegalStateException("transformer profile not found: " + name);

		LogTransformerFactory factory = factoryRegistry.getFactory(profile.getFactoryName());
		if (factory == null)
			throw new IllegalStateException("transformer factory not found: " + profile.getFactoryName());

		return factory.newTransformer(profile.getConfigs());
	}
}
