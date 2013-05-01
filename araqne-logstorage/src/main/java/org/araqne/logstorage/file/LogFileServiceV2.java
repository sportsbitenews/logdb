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
package org.araqne.logstorage.file;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logstorage.LogFileService;
import org.araqne.logstorage.LogFileServiceRegistry;

@Component(name = "logstorage-log-file-service-v2")
public class LogFileServiceV2 implements LogFileService {
	@Requires
	private LogFileServiceRegistry registry;

	private static final String OPT_TABLE_NAME = "tableName";
	private static final String OPT_INDEX_PATH = "indexPath";
	private static final String OPT_DATA_PATH = "dataPath";

	public static class Option extends TreeMap<String, Object> {
		private static final long serialVersionUID = 1L;

		public Option(String tableName, File indexPath, File dataPath) {
			this.put(OPT_TABLE_NAME, tableName);
			this.put(OPT_INDEX_PATH, indexPath);
			this.put(OPT_DATA_PATH, dataPath);
		}
	}

	@Validate
	public void start() {
		registry.register(this);
	}

	@Invalidate
	public void stop() {
		if (registry != null)
			registry.unregister(this);
	}

	@Override
	public String getType() {
		return "v2";
	}

	@Override
	public long count(File f) {
		return LogCounterV2.count(f);
	}

	@Override
	public LogFileWriter newWriter(Map<String, Object> options) {
		checkOption(options);
		File indexPath = (File) options.get(OPT_INDEX_PATH);
		File dataPath = (File) options.get(OPT_DATA_PATH);
		try {
			return new LogFileWriterV2(indexPath, dataPath);
		} catch (Throwable t) {
			throw new IllegalStateException("cannot open writer v2: data file - " + dataPath.getAbsolutePath(), t);
		}
	}

	private void checkOption(Map<String, Object> options) {
		for (String key : new String[] { OPT_INDEX_PATH, OPT_DATA_PATH }) {
			if (!options.containsKey(key))
				throw new IllegalArgumentException("LogFileServiceV1: " + key + " must be supplied");
		}
	}

	@Override
	public LogFileReader newReader(Map<String, Object> options) {
		checkOption(options);
		File indexPath = (File) options.get(OPT_INDEX_PATH);
		File dataPath = (File) options.get(OPT_DATA_PATH);
		try {
			return new LogFileReaderV2(indexPath, dataPath);
		} catch (Throwable t) {
			throw new IllegalStateException("cannot open reader v2: data file - " + dataPath.getAbsolutePath());
		}
	}

	@Override
	public Map<String, String> getConfigs() {
		return new HashMap<String, String>();
	}

	@Override
	public void setConfig(String key, String value) {
	}

	@Override
	public void unsetConfig(String key) {
	}

}
