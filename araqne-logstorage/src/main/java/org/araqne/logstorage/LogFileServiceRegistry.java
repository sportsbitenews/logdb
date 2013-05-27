package org.araqne.logstorage;

import java.util.Map;

import org.araqne.logstorage.file.LogFileReader;
import org.araqne.logstorage.file.LogFileWriter;

public interface LogFileServiceRegistry {
	void register(LogFileService service);

	void unregister(LogFileService service);

	String[] getServiceTypes();

	LogFileWriter newWriter(String type, Map<String, Object> options);

	LogFileReader newReader(String tableName, String type, Map<String, Object> options);

	LogFileService getLogFileService(String type);

	/**
	 * @since 1.16.0
	 */
	String[] getInstalledTypes();

	/**
	 * @since 1.16.0
	 */
	void uninstall(String type);
}
