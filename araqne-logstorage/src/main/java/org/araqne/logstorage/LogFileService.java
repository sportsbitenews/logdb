package org.araqne.logstorage;

import java.util.Map;

import org.araqne.logstorage.file.LogFileReader;
import org.araqne.logstorage.file.LogFileWriter;

public interface LogFileService {
	String getType();

	LogFileWriter newWriter(Map<String, Object> options);

	LogFileReader newReader(Map<String, Object> options);
}
