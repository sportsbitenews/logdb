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

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.araqne.logdb.cep.EventContextService;
import org.araqne.logdb.cep.EventContextStorage;

@Component(name = "event-ctx-service")
@Provides
public class DefaultEventContextService implements EventContextService {

	private ConcurrentHashMap<String, EventContextStorage> storages = new ConcurrentHashMap<String, EventContextStorage>();

	@Override
	public List<EventContextStorage> getStorages() {
		return new ArrayList<EventContextStorage>(storages.values());
	}

	@Override
	public EventContextStorage getStorage(String name) {
		return storages.get(name);
	}

	@Override
	public void registerStorage(EventContextStorage storage) {
		EventContextStorage old = storages.putIfAbsent(storage.getName(), storage);
		if (old != null)
			throw new IllegalStateException("duplicated event context storage: " + storage.getName());
	}

	@Override
	public void unregisterStorage(EventContextStorage storage) {
		storages.remove(storage.getName(), storage);
	}

}
