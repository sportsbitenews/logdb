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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logstorage.LogFileService;
import org.araqne.logstorage.LogFileServiceRegistry;
import org.araqne.logstorage.LogFlushCallback;
import org.araqne.logstorage.LogFlushCallbackArgs;
import org.araqne.logstorage.TableConfigSpec;
import org.araqne.storage.api.FilePath;
import org.araqne.storage.localfile.LocalFilePath;

@Component(name = "logstorage-log-file-service-v1")
public class LogFileServiceV1 implements LogFileService {
	@Requires
	private LogFileServiceRegistry registry;

	private static final String OPT_INDEX_PATH = "indexPath";
	private static final String OPT_DATA_PATH = "dataPath";
	private static final String OPT_TABLE_NAME = "tableName";
	private static final String OPT_FLUSH_CALLBACKS = "flushCallbacks";
	
	public static class Option extends TreeMap<String, Object> {
		private static final long serialVersionUID = 1L;

		public Option(FilePath indexPath, FilePath dataPath) {
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
		return "v1";
	}

	@Override
	public long count(FilePath f) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public LogFileWriter newWriter(Map<String, Object> options) {
		checkOption(options);
		LocalFilePath indexPath = (LocalFilePath) options.get(OPT_INDEX_PATH);
		LocalFilePath dataPath = (LocalFilePath) options.get(OPT_DATA_PATH);
		String tableName = (String) options.get(OPT_TABLE_NAME);
		Set<LogFlushCallback> flushCallbacks = (Set<LogFlushCallback>) options.get(OPT_FLUSH_CALLBACKS);
		try {
			return new LogFileWriterV1(indexPath.getFile(), dataPath.getFile(), flushCallbacks, new LogFlushCallbackArgs(tableName));
		} catch (Throwable t) {
			throw new IllegalStateException("cannot open writer: data file " + dataPath.getAbsolutePath(), t);
		}
	}

	private void checkOption(Map<String, Object> options) {
		for (String key : new String[] { OPT_INDEX_PATH, OPT_DATA_PATH }) {
			if (!options.containsKey(key))
				throw new IllegalArgumentException("LogFileServiceV1: " + key + " must be supplied");
		}
	}

	@Override
	public LogFileReader newReader(String tableName, Map<String, Object> options) {
		checkOption(options);
		FilePath indexPath = (FilePath) options.get(OPT_INDEX_PATH);
		FilePath dataPath = (FilePath) options.get(OPT_DATA_PATH);
		try {
			return new LogFileReaderV1(tableName, indexPath, dataPath);
		} catch (Throwable t) {
			throw new IllegalStateException("cannot open reader v1: data file - " + dataPath.getAbsolutePath(), t);
		}
	}

	@Override
	public List<TableConfigSpec> getConfigSpecs() {
		return Arrays.asList();
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

	@Override
	public List<TableConfigSpec> getReplicaConfigSpecs() {
		return Arrays.asList();
	}

	@Override
	public List<TableConfigSpec> getSecondaryConfigSpecs() {
		return Arrays.asList();
	}

}
