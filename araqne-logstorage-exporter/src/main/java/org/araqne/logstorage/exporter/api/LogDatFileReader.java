package org.araqne.logstorage.exporter.api;

import java.util.List;
import java.util.Map;

public interface LogDatFileReader {
	boolean hasNext();

	List<Map<String, Object>> nextBlock();

	void close();
}
