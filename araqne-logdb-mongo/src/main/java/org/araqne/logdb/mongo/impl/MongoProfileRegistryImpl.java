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
package org.araqne.logdb.mongo.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.araqne.logdb.mongo.MongoProfile;
import org.araqne.logdb.mongo.MongoProfileRegistry;

@Component(name = "mongo-profile-registry")
@Provides
public class MongoProfileRegistryImpl implements MongoProfileRegistry {

	private ConcurrentHashMap<String, MongoProfile> profiles = new ConcurrentHashMap<String, MongoProfile>();

	@Override
	public List<MongoProfile> getProfiles() {
		return new ArrayList<MongoProfile>(profiles.values());
	}

	@Override
	public MongoProfile getProfile(String name) {
		return profiles.get(name);
	}

	@Override
	public void createProfile(MongoProfile profile) {
		MongoProfile old = profiles.putIfAbsent(profile.getName(), profile);
		if (old != null)
			throw new IllegalStateException("duplicated mongo profile: " + profile.getName());
	}

	@Override
	public void removeProfile(String name) {
		MongoProfile old = profiles.remove(name);
		if (old == null)
			throw new IllegalStateException("mongo profile not found: " + name);
	}

}
