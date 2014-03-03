package org.araqne.logstorage;

import java.io.File;
import java.util.Map;

import org.araqne.logstorage.file.LogFileReader;
import org.araqne.logstorage.file.LogFileWriter;

public interface LogFileService {
	String getType();

	/**
	 * @since 1.17.0
	 * @param f
	 *            .idx file path
	 * @return log count
	 */
	long count(File f);

	LogFileWriter newWriter(Map<String, Object> options);

	LogFileReader newReader(String tableName, Map<String, Object> options);

	Map<String, String> getConfigs();

	void setConfig(String key, String value);

	void unsetConfig(String key);
}
