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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.confdb.ConfigService;
import org.araqne.logstorage.LogFileService;
import org.araqne.logstorage.LogFileServiceRegistry;
import org.araqne.logstorage.LogFlushCallback;
import org.araqne.logstorage.LogFlushCallbackArgs;
import org.araqne.logstorage.LogTableRegistry;
import org.araqne.logstorage.TableConfigSpec;
import org.araqne.logstorage.TableSchema;
import org.araqne.logstorage.engine.ConfigUtil;
import org.araqne.logstorage.engine.Constants;
import org.araqne.storage.api.FilePath;
import org.araqne.storage.api.FilePathNameFilter;
import org.araqne.storage.api.StorageManager;
import org.araqne.storage.localfile.LocalFilePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name = "logstorage-log-file-service-v1")
public class LogFileServiceV1 implements LogFileService {
	private final Logger logger = LoggerFactory.getLogger(LogFileServiceV1.class);

	@Requires
	private ConfigService conf;

	@Requires
	private LogFileServiceRegistry registry;

	@Requires
	private StorageManager storageManager;

	@Requires
	private LogTableRegistry tableRegistry;

	private static final String OPT_INDEX_PATH = "indexPath";
	private static final String OPT_DATA_PATH = "dataPath";
	private static final String OPT_TABLE_NAME = "tableName";
	private static final String OPT_FLUSH_CALLBACKS = "flushCallbacks";

	private FilePath logDir;

	public static class Option extends TreeMap<String, Object> {
		private static final long serialVersionUID = 1L;

		public Option(FilePath indexPath, FilePath dataPath) {
			this.put(OPT_INDEX_PATH, indexPath);
			this.put(OPT_DATA_PATH, dataPath);
		}
	}

	@Validate
	public void start() {
		logDir = storageManager.resolveFilePath(System.getProperty("araqne.data.dir")).newFilePath("araqne-logstorage/log");
		logDir = storageManager.resolveFilePath(getStringParameter(Constants.LogStorageDirectory, logDir.getAbsolutePath()));
		logDir.mkdirs();
		
		registry.register(this);
	}

	private String getStringParameter(Constants key, String defaultValue) {
		String value = ConfigUtil.get(conf, key);
		if (value != null)
			return value;
		return defaultValue;
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
	public List<Date> getPartitions(String tableName) {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		TableSchema schema = tableRegistry.getTableSchema(tableName, true);

		FilePath baseDir = logDir;
		if (schema.getPrimaryStorage().getBasePath() != null)
			baseDir = storageManager.resolveFilePath(schema.getPrimaryStorage().getBasePath());

		FilePath tableDir = baseDir.newFilePath(Integer.toString(schema.getId()));

		FilePath[] files = tableDir.listFiles(new FilePathNameFilter() {
			@Override
			public boolean accept(FilePath dir, String name) {
				return name.endsWith(".idx");
			}
		});

		List<Date> dates = new ArrayList<Date>();
		if (files != null) {
			for (FilePath file : files) {
				try {
					dates.add(dateFormat.parse(file.getName().split("\\.")[0]));
				} catch (ParseException e) {
					logger.error("araqne logstorage: invalid log filename, table {}, {}", tableName, file.getName());
				}
			}
		}

		Collections.sort(dates, Collections.reverseOrder());

		return dates;
	}

	@Override
	public LogFileWriter newWriter(Map<String, Object> options) {
		checkOption(options);
		LocalFilePath indexPath = (LocalFilePath) options.get(OPT_INDEX_PATH);
		LocalFilePath dataPath = (LocalFilePath) options.get(OPT_DATA_PATH);
		String tableName = (String) options.get(OPT_TABLE_NAME);
		Set<LogFlushCallback> flushCallbacks = (Set<LogFlushCallback>) options.get(OPT_FLUSH_CALLBACKS);
		try {
			return new LogFileWriterV1(indexPath.getFile(), dataPath.getFile(), flushCallbacks, new LogFlushCallbackArgs(
					tableName));
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
