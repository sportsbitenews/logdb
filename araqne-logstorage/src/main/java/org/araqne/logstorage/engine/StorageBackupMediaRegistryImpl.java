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
package org.araqne.logstorage.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.araqne.logstorage.backup.StorageBackupMediaFactory;
import org.araqne.logstorage.backup.StorageBackupMediaRegistry;

/**
 * @since 2.3.0
 * @author xeraph
 * 
 */
@Component(name = "logstorage-backup-media-registry")
@Provides
public class StorageBackupMediaRegistryImpl implements StorageBackupMediaRegistry {
	private ConcurrentHashMap<String, StorageBackupMediaFactory> factories;

	public StorageBackupMediaRegistryImpl() {
		this.factories = new ConcurrentHashMap<String, StorageBackupMediaFactory>();
	}

	@Override
	public List<StorageBackupMediaFactory> getFactories() {
		return new ArrayList<StorageBackupMediaFactory>(factories.values());
	}

	@Override
	public StorageBackupMediaFactory getFactory(String name) {
		return factories.get(name);
	}

	@Override
	public void registerFactory(StorageBackupMediaFactory factory) {
		if (factory == null)
			throw new IllegalArgumentException("factory should not be null");

		StorageBackupMediaFactory old = factories.putIfAbsent(factory.getName(), factory);
		if (old != null)
			throw new IllegalStateException("duplicated storage media factory: " + factory.getName());
	}

	@Override
	public void unregisterFactory(StorageBackupMediaFactory factory) {
		if (factory == null)
			throw new IllegalArgumentException("factory should not be null");

		factories.remove(factory.getName(), factory);
	}
}
