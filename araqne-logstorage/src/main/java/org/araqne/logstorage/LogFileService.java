package org.araqne.logstorage;

import java.util.Map;

import org.araqne.logstorage.file.LogFileReader;
import org.araqne.logstorage.file.LogFileWriter;
import org.araqne.storage.api.FilePath;

public interface LogFileService {
	String getType();

	/**
	 * @since 1.17.0
	 * @param f
	 *            .idx file path
	 * @return log count
	 */
	long count(FilePath f);

	LogFileWriter newWriter(Map<String, Object> options);

	LogFileReader newReader(String tableName, Map<String, Object> options);
	
	LogFileRepairer newRepairer();

	Map<String, String> getConfigs();

	void setConfig(String key, String value);

	void unsetConfig(String key);
}
