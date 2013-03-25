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

@Component(name = "logstorage-log-file-service-v1")
public class LogFileServiceV1 implements LogFileService {
	@Requires
	private LogFileServiceRegistry registry;

	private static final String OPT_INDEX_PATH = "indexPath";
	private static final String OPT_DATA_PATH = "dataPath";

	public static class Option extends TreeMap<String, Object> {
		private static final long serialVersionUID = 1L;

		public Option(File indexPath, File dataPath) {
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
	public LogFileWriter newWriter(Map<String, Object> options) {
		checkOption(options);
		File indexPath = (File) options.get(OPT_INDEX_PATH);
		File dataPath = (File) options.get(OPT_DATA_PATH);
		try {
			return new LogFileWriterV1(indexPath, dataPath);
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
	public LogFileReader newReader(Map<String, Object> options) {
		checkOption(options);
		File indexPath = (File) options.get(OPT_INDEX_PATH);
		File dataPath = (File) options.get(OPT_DATA_PATH);
		try {
			return new LogFileReaderV1(indexPath, dataPath);
		} catch (Throwable t) {
			throw new IllegalStateException("cannot open reader v1: data file - " + dataPath.getAbsolutePath(), t);
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
