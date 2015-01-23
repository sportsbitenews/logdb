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
package org.araqne.logstorage.file;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.confdb.Config;
import org.araqne.confdb.ConfigCollection;
import org.araqne.confdb.ConfigDatabase;
import org.araqne.confdb.ConfigIterator;
import org.araqne.confdb.ConfigService;
import org.araqne.cron.MinutelyJob;
import org.araqne.logstorage.*;
import org.araqne.logstorage.file.LogFileReader;
import org.araqne.logstorage.file.LogFileWriter;
import org.araqne.storage.api.FilePath;
import org.araqne.storage.localfile.LocalFilePath;
import org.araqne.storage.api.FilePathNameFilter;
import org.araqne.storage.api.StorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@MinutelyJob
@Component(name = "logstorage-log-file-service-v3", immediate = true)
@Provides(specifications = { Runnable.class })
public class LogFileServiceV3o implements Runnable, LogFileService, LogStatsListener {
	private final Logger logger = LoggerFactory.getLogger(LogFileServiceV3o.class);
	public static final String SLOT_COUNT = "slot_count";
	public static final String FLUSH_COUNT = "flush_count";

	private static final int DEFAULT_FLUSH_COUNT = 2000;

	@Requires
	private LogFileServiceRegistry registry;

	@Requires
	private StorageManager storageManager;

	@Requires
	private LogTableRegistry tableRegistry;

	@Requires
	private ConfigService conf;

	@Requires
	private LogStorage storage;

	private FilePath logDir;

	private Map<String, String> configs = new ConcurrentHashMap<String, String>();

	// for stats counting
	private ConcurrentHashMap<String, LogTableStatus> loggers = new ConcurrentHashMap<String, LogTableStatus>();

	public LogFileServiceV3o() {
		super();
	}

	private static final String OPT_STORAGE_CONFIG = "storageConfig";
	private static final String OPT_TABLE_NAME = "tableName";
	private static final String OPT_DAY = "day";
	private static final String OPT_INDEX_PATH = "indexPath";
	private static final String OPT_DATA_PATH = "dataPath";
	private static final String OPT_COMPRESSION = "compression";
	private static final String OPT_CALLBACK_SET = "callbackSet";

	public static class Option extends TreeMap<String, Object> {
		private static final long serialVersionUID = 1L;

		public Option(FilePath indexPath, FilePath dataPath) {
			this.put(OPT_INDEX_PATH, indexPath);
			this.put(OPT_DATA_PATH, dataPath);
		}
	}

	@SuppressWarnings("unchecked")
	@Validate
	public void start() {
		logDir = storageManager.resolveFilePath(System.getProperty("araqne.data.dir")).newFilePath("araqne-logstorage/log");
		logDir = storageManager.resolveFilePath(getStringParameter("log_storage_dir", logDir.getAbsolutePath()));
		logDir.mkdirs();

		loggers.clear();

		ConfigDatabase db = conf.ensureDatabase("logpresso-logstorage");
		ConfigCollection col = db.ensureCollection("configs");
		Config c = col.findOne(null);
		if (c != null)
			configs.putAll((Map<String, String>) c.getDocument());

		registry.register(this);
	}

	private String getStringParameter(String key, String defaultValue) {
		ConfigDatabase db = conf.ensureDatabase("araqne-logstorage");
		ConfigCollection col = db.ensureCollection("global_settings");
		Config c = getGlobalConfig(col);
		if (c == null)
			return defaultValue;

		@SuppressWarnings("unchecked")
		Map<String, Object> m = (Map<String, Object>) c.getDocument();
		String value = (String) m.get(key);
		if (value != null)
			return value;
		return defaultValue;
	}

	private static Config getGlobalConfig(ConfigCollection col) {
		ConfigIterator it = col.findAll();
		try {
			if (!it.hasNext())
				return null;

			return it.next();
		} finally {
			it.close();
		}
	}

	@Invalidate
	public void stop() {
		if (registry != null)
			registry.unregister(this);
	}

	@Override
	public String getType() {
		return "v3";
	}

	@Override
	public long count(FilePath f) {
		return new LogCounterV3o().getCount(f);
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

		StorageConfig storageConfig = (StorageConfig) options.get(OPT_STORAGE_CONFIG);
		String tableName = (String) options.get(OPT_TABLE_NAME);
		Date day = (Date) options.get(OPT_DAY);
		FilePath indexPath = getFilePath(options, OPT_INDEX_PATH);
		FilePath dataPath = getFilePath(options, OPT_DATA_PATH);
		String compression = getStorageConfigValue(storageConfig, OPT_COMPRESSION);
		CallbackSet callbackSet = (CallbackSet) options.get(OPT_CALLBACK_SET);

		if (compression == null)
			compression = "deflater";

		int flushCount = DEFAULT_FLUSH_COUNT;
		if (configs.containsKey(FLUSH_COUNT))
			flushCount = Integer.valueOf(configs.get(FLUSH_COUNT));

		logger.debug("logpresso logstorage: new writer with flush count [{}]", flushCount);

		try {
			LogWriterConfigV3o config = new LogWriterConfigV3o();
			config.setTableName(tableName);
			config.setIndexPath(indexPath);
			config.setDataPath(dataPath);
			config.setListener(this);
			config.setFlushCount(flushCount);
			config.setCompression(compression);
			config.setDay(day);
			config.setCallbackSet(callbackSet);
			config.setListener(this);
			return new LogFileWriterV3o(config);
		} catch (Throwable t) {
			throw new IllegalStateException("cannot open writer v3: data file - " + dataPath.getAbsolutePath(), t);
		}
	}

	private String getStorageConfigValue(StorageConfig storageConfig, String name) {
		if (storageConfig == null)
			return null;

		TableConfig config = storageConfig.getConfig(name);
		if (config == null)
			return null;

		return config.getValue();
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

	private void checkOption(Map<String, Object> options) {
		for (String key : new String[] { OPT_INDEX_PATH, OPT_DATA_PATH }) {
			if (!options.containsKey(key))
				throw new IllegalArgumentException("LogFileServiceV1: " + key + " must be supplied");
		}
	}

	@Override
	public LogFileReader newReader(String tableName, Map<String, Object> options) {
		checkOption(options);
		FilePath indexPath = getFilePath(options, OPT_INDEX_PATH);
		FilePath dataPath = getFilePath(options, OPT_DATA_PATH);
		Date day = (Date) options.get("day");

		try {
			LogReaderConfigV3o c = new LogReaderConfigV3o();
			c.tableName = tableName;
			c.indexPath = indexPath;
			c.dataPath = dataPath;
			c.checkIntegrity = false;
			c.day = day;
			return new LogFileReaderV3o(c);
		} catch (Throwable t)
		{
			throw new IllegalStateException("cannot open reader v3: data file - " +
					dataPath.getAbsolutePath(), t);
		}
	}

	@Override
	public List<TableConfigSpec> getConfigSpecs() {
		TableConfigSpec compression = new TableConfigSpec();
		compression.setKey("compression");
		compression.setDisplayNames(TableConfigSpec.locales("Compression", "압축 방식"));
		compression.setDescriptions(TableConfigSpec.locales("deflate or snappy", "deflate 혹은 snappy"));
		compression.setOptional(true);
		compression.setValidator(new EnumConfigValidator(new HashSet<String>(Arrays.asList("deflate", "snappy"))));
		compression.setEnums("deflate:snappy");

		return Arrays.asList(compression);
	}

	@Override
	public Map<String, String> getConfigs() {
		return new HashMap<String, String>(configs);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void setConfig(String key, String value) {
		ConfigDatabase db = conf.ensureDatabase("logpresso-logstorage");
		ConfigCollection col = db.ensureCollection("configs");
		Config c = col.findOne(null);
		if (c != null) {
			Map<String, Object> m = (Map<String, Object>) c.getDocument();
			m.put(key, value);
			c.setDocument(m);
			c.update();
		} else {
			Map<String, Object> m = new HashMap<String, Object>();
			m.put(key, value);
			col.add(m);
		}

		configs.put(key, value);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void unsetConfig(String key) {
		configs.remove(key);

		ConfigDatabase db = conf.ensureDatabase("logpresso-logstorage");
		ConfigCollection col = db.ensureCollection("configs");
		Config c = col.findOne(null);
		if (c != null) {
			Map<String, Object> m = (Map<String, Object>) c.getDocument();
			m.remove(key);
			c.setDocument(m);
			c.update();
		}
	}

	/**
	 * @since 2.1.0
	 */
	@Override
	public void onWrite(LogStats stats) {
		String tableName = stats.getTableName();

		// prevent loop
		if (tableName.equals("logpresso-log-trend") || tableName.startsWith("$result$"))
			return;

		LogTableStatus newStatus = new LogTableStatus(stats.getLogCount(), stats.getOriginalDataSize(),
				stats.getCompressedDataSize());

		LogTableStatus old = loggers.putIfAbsent(tableName, newStatus);
		if (old != null) {
			old.count.addAndGet(stats.getLogCount());
			old.volume.addAndGet(stats.getOriginalDataSize());
			old.compressed.addAndGet(stats.getCompressedDataSize());
		}
	}

	/**
	 * @since 2.1.0
	 */
	@Override
	public void run() {
		writeTrends();
	}

	private void writeTrends() {
		for (Entry<String, LogTableStatus> e : loggers.entrySet()) {
			Map<String, Object> m = new HashMap<String, Object>();
			long count = e.getValue().count.getAndSet(0);
			long volume = e.getValue().volume.getAndSet(0);
			long compressed = e.getValue().compressed.getAndSet(0);
			if (count == 0 && volume == 0 && compressed == 0)
				continue;

			m.put("table", e.getKey());
			m.put("count", count);
			m.put("volume", volume);
			m.put("compressed", compressed);
			Log log = new Log("logpresso-log-trend", new Date(), m);
			try {
				storage.write(log);
			} catch (InterruptedException ex) {
				logger.warn("trend log has been discarded by interrupt: {}", m);
			} catch (TableNotFoundException ex) {
				storage.ensureTable(new TableSchema("logpresso-log-trend", new StorageConfig("v2")));
				try {
					storage.write(log);
				} catch (InterruptedException e1) {
				}
			}
		}
	}

	/**
	 * @since 2.1.0
	 */
	private static class LogTableStatus {
		private AtomicLong count;
		private AtomicLong volume;
		private AtomicLong compressed;

		public LogTableStatus(long count, long volume, long compressed) {
			this.count = new AtomicLong(count);
			this.volume = new AtomicLong(volume);
			this.compressed = new AtomicLong(compressed);
		}
	}

	@Override
	public List<TableConfigSpec> getReplicaConfigSpecs() {
		TableConfigSpec replicationMode = new TableConfigSpec();
		replicationMode.setKey("replication_mode");
		replicationMode.setDisplayNames(TableConfigSpec.locales("Replication Mode", "복제 모드"));
		replicationMode.setDescriptions(TableConfigSpec.locales("active or standby", "active 혹은 standby"));
		replicationMode.setOptional(true);
		replicationMode.setUpdatable(true);

		TableConfigSpec replicationTable = new TableConfigSpec();
		replicationTable.setKey("replication_table");
		replicationTable.setDisplayNames(TableConfigSpec.locales("Replication Table", "복제 대상 테이블"));
		replicationTable.setDescriptions(TableConfigSpec.locales("node\\table format", "노드이름\\테이블이름 형식"));
		replicationTable.setOptional(true);
		replicationTable.setUpdatable(true);

		return Arrays.asList(replicationMode, replicationTable);
	}

	@Override
	public List<TableConfigSpec> getSecondaryConfigSpecs() {
		return Arrays.asList();
	}

}
