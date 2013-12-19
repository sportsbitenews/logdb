package org.araqne.logstorage.exporter.api;

import java.util.Map;

public interface LogWriter {

	void write(Map<String, Object> log);

	void close();
}
