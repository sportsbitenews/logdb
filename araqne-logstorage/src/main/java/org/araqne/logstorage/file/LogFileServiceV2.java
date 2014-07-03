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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logstorage.CallbackSet;
import org.araqne.logstorage.LogFileService;
import org.araqne.logstorage.LogFileServiceRegistry;
import org.araqne.logstorage.StorageConfig;
import org.araqne.logstorage.LogTableRegistry;
import org.araqne.logstorage.TableConfigSpec;
import org.araqne.logstorage.TableSchema;
import org.araqne.storage.api.FilePath;
import org.araqne.storage.api.FilePathNameFilter;
import org.araqne.storage.api.StorageManager;
import org.araqne.storage.localfile.LocalFilePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name = "logstorage-log-file-service-v2", immediate = true)
public class LogFileServiceV2 implements LogFileService {
	private final Logger logger = LoggerFactory.getLogger(LogFileServiceV2.class);

	private final LogTableRegistry tableRegistry;

	private StorageManager storageManager;

	private static final String OPT_STORAGE_CONFIG = "storageConfig";
	private static final String OPT_TABLE_NAME = "tableName";
	private static final String OPT_DAY = "day";
	private static final String OPT_BASE_PATH = "basePath";
	private static final String OPT_INDEX_PATH = "indexPath";
	private static final String OPT_DATA_PATH = "dataPath";
	private static final String OPT_KEY_PATH = "keyPath";
	private static final Object OPT_CALLBACK_SET = "callbackSet";

	private final FilePath logDir;

	public static class Option extends TreeMap<String, Object> {
		private static final long serialVersionUID = 1L;

		public Option(StorageConfig config, Map<String, String> tableMetadata, String tableName, FilePath basePath,
				FilePath indexPath, FilePath dataPath, FilePath keyPath) {
			this.put(OPT_STORAGE_CONFIG, config);
			this.putAll(tableMetadata);
			this.put(OPT_TABLE_NAME, tableName);
			this.put(OPT_BASE_PATH, basePath);
			this.put(OPT_INDEX_PATH, indexPath);
			this.put(OPT_DATA_PATH, dataPath);
			this.put(OPT_KEY_PATH, keyPath);
		}
	}

	@Requires
	private LogFileServiceRegistry registry;

	public LogFileServiceV2(LogTableRegistry tableRegistry, StorageManager storageManager, FilePath logDir) {
		this.tableRegistry = tableRegistry;
		this.storageManager = storageManager;
		this.logDir = logDir;
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
	public long count(FilePath f) {
		return LogCounterV2.count(f);
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
		String tableName = (String) options.get(OPT_TABLE_NAME);
		Date day = (Date) options.get(OPT_DAY);
		FilePath indexPath = getFilePath(options, OPT_INDEX_PATH);
		FilePath dataPath = getFilePath(options, OPT_DATA_PATH);
		CallbackSet cbSet = (CallbackSet) options.get(OPT_CALLBACK_SET);
		try {
			return new LogFileWriterV2(indexPath, dataPath, cbSet, tableName, day);
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

	private FilePath getFilePath(Map<String, Object> options, String optName) {
		Object obj = options.get(optName);
		if (obj == null)
			return (FilePath) obj;
		else if (obj instanceof File) {
			return new LocalFilePath((File) obj);
		} else {
			return (FilePath) obj;
		}
	}

	@Override
	public LogFileReader newReader(String tableName, Map<String, Object> options) {
		checkOption(options);
		FilePath indexPath = getFilePath(options, OPT_INDEX_PATH);
		FilePath dataPath = getFilePath(options, OPT_DATA_PATH);
		try {
			return new LogFileReaderV2(tableName, indexPath, dataPath);
		} catch (Throwable t) {
			throw new IllegalStateException("cannot open reader v2: data file - " + dataPath.getAbsolutePath());
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
