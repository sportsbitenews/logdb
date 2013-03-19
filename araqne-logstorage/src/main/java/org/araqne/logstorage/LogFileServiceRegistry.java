package org.araqne.logstorage;

import java.util.Map;

import org.araqne.logstorage.file.LogFileReader;
import org.araqne.logstorage.file.LogFileWriter;

public interface LogFileServiceRegistry {
	void register(LogFileService service);

	void unregister(LogFileService service);

	String[] getServiceTypes();

	LogFileWriter newWriter(String type, Map<String, Object> options) throws Exception;

	LogFileReader newReader(String type, Map<String, Object> options) throws Exception;

	LogFileService getLogFileService(String type);
}
