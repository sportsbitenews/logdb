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
package org.araqne.logdb.jms.impl;

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
import org.araqne.confdb.ConfigService;
import org.araqne.confdb.Predicates;
import org.araqne.logdb.jms.JmsProfile;
import org.araqne.logdb.jms.JmsProfileRegistry;

@Component(name = "jms-profile-registry")
@Provides
public class JmsProfileRegistryImpl implements JmsProfileRegistry {

	private static final String DBNAME = "araqne-logdb-jms";

	@Requires
	private ConfigService conf;

	private ConcurrentMap<String, JmsProfile> profiles = new ConcurrentHashMap<String, JmsProfile>();

	@Validate
	public void start() {
		ConfigDatabase db = conf.ensureDatabase(DBNAME);
		for (JmsProfile profile : db.findAll(JmsProfile.class).getDocuments(JmsProfile.class)) {
			profiles.put(profile.getName(), profile);
		}
	}

	@Invalidate
	public void stop() {
		profiles.clear();
	}

	@Override
	public List<JmsProfile> getProfiles() {
		return new ArrayList<JmsProfile>(profiles.values());
	}

	@Override
	public JmsProfile getProfile(String name) {
		return profiles.get(name);
	}

	@Override
	public void createProfile(JmsProfile profile) {
		JmsProfile old = profiles.putIfAbsent(profile.getName(), profile);
		if (old != null)
			throw new IllegalStateException("duplicated jms profile name: " + profile.getName());

		ConfigDatabase db = conf.ensureDatabase(DBNAME);
		db.add(profile);
	}

	@Override
	public void removeProfile(String name) {
		ConfigDatabase db = conf.ensureDatabase(DBNAME);
		Config c = db.findOne(JmsProfile.class, Predicates.field("name", name));
		if (c != null)
			c.remove();

		JmsProfile p = profiles.remove(name);
		if (p == null)
			throw new IllegalStateException("jms profile not found: " + name);
	}
}
