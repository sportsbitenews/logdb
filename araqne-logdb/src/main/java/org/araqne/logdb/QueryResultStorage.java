package org.araqne.logdb;

import java.io.IOException;

import org.araqne.logstorage.file.LogFileReader;
import org.araqne.logstorage.file.LogFileWriter;

public interface QueryResultStorage {
	LogFileWriter createWriter(QueryResultConfig config) throws IOException;

	LogFileReader createReader(QueryResultConfig config) throws IOException;
}
