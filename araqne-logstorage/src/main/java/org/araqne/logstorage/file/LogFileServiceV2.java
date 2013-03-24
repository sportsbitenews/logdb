package org.araqne.logstorage.file;

import java.io.File;
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
		return "v2";
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

}
