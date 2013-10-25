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

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logstorage.backup.FileStorageBackupMedia;
import org.araqne.logstorage.backup.StorageBackupConfigSpec;
import org.araqne.logstorage.backup.StorageBackupMedia;
import org.araqne.logstorage.backup.StorageBackupMediaFactory;
import org.araqne.logstorage.backup.StorageBackupMediaRegistry;

/**
 * @since 2.3.0
 * @author xeraph
 * 
 */
@Component(name = "logstorage-file-backup-media-factory")
public class FileStorageBackupMediaFactory implements StorageBackupMediaFactory {

	@Requires
	private StorageBackupMediaRegistry registry;

	@Override
	public String getName() {
		return "local";
	}

	@Validate
	public void start() {
		registry.registerFactory(this);
	}

	@Invalidate
	public void stop() {
		if (registry != null)
			registry.unregisterFactory(this);
	}

	@Override
	public List<StorageBackupConfigSpec> getConfigSpecs() {
		StorageBackupConfigSpec path = new StorageBackupConfigSpec();
		path.setKey("path");
		path.setDisplayNames(t("Backup Path"));
		path.setDescriptions(t("Local file system path"));

		return Arrays.asList(path);
	}

	private Map<Locale, String> t(String en) {
		Map<Locale, String> m = new HashMap<Locale, String>();
		m.put(Locale.ENGLISH, en);
		return m;
	}

	@Override
	public StorageBackupMedia newMedia(Map<String, String> configs) {
		File backupPath = new File(configs.get("path"));
		if (!backupPath.exists() || !backupPath.isDirectory()) {
			throw new IllegalStateException("invalid backup path: " + backupPath.getAbsolutePath());
		}

		return new FileStorageBackupMedia(backupPath);
	}
}
