/**
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
package org.araqne.log.api.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.confdb.Config;
import org.araqne.confdb.ConfigDatabase;
import org.araqne.confdb.ConfigService;
import org.araqne.confdb.Predicates;
import org.araqne.log.api.ParserSelectorProfile;
import org.araqne.log.api.ParserSelectorProvider;
import org.araqne.log.api.ParserSelectorRegistry;

@Component(name = "parser-selector-registry")
@Provides
public class ParserSelectorRegistryImpl implements ParserSelectorRegistry {

	@Requires
	private ConfigService conf;

	private ConcurrentHashMap<String, ParserSelectorProfile> profiles;
	private ConcurrentHashMap<String, ParserSelectorProvider> providers;

	@Validate
	public void start() {
		profiles = new ConcurrentHashMap<String, ParserSelectorProfile>();
		providers = new ConcurrentHashMap<String, ParserSelectorProvider>();
	}

	@Invalidate
	public void stop() {
		profiles.clear();
		providers.clear();
	}

	@Override
	public List<ParserSelectorProfile> getProfiles() {
		return new ArrayList<ParserSelectorProfile>(profiles.values());
	}

	@Override
	public ParserSelectorProfile getProfile(String name) {
		if (name == null)
			return null;
		return profiles.get(name);
	}

	@Override
	public synchronized void createProfile(ParserSelectorProfile profile) {
		ParserSelectorProfile old = profiles.putIfAbsent(profile.getName(), profile);
		if (old != null)
			throw new IllegalStateException("duplicated parser selector profile: " + profile.getName());

		ConfigDatabase db = conf.ensureDatabase("araqne-log-api");
		db.add(profile);

	}

	@Override
	public synchronized void removeProfile(String name) {
		ConfigDatabase db = conf.ensureDatabase("araqne-log-api");
		Config c = db.findOne(ParserSelectorProfile.class, Predicates.field("name", name));
		if (c != null)
			c.remove();

		ParserSelectorProfile old = profiles.remove(name);
		if (old == null)
			throw new IllegalStateException("parser selector profile not found: " + name);
	}

	@Override
	public List<ParserSelectorProvider> getProviders() {
		return new ArrayList<ParserSelectorProvider>(providers.values());
	}

	@Override
	public ParserSelectorProvider getProvider(String name) {
		return providers.get(name);
	}

	@Override
	public void addProvider(ParserSelectorProvider provider) {
		ParserSelectorProvider old = providers.putIfAbsent(provider.getName(), provider);
		if (old != null)
			throw new IllegalStateException("duplicated parser selector provider: " + provider.getName());
	}

	@Override
	public void removeProvider(ParserSelectorProvider provider) {
		providers.remove(provider.getName(), provider);
	}
}
