package org.araqne.logdb;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.araqne.logstorage.LogStorage;

public interface DataUploadService {
	void loadTextFile(LogStorage storage, String ticket, boolean last, String data, String datePattern, String dateFormat,
			String dateLocale, String beginRegex, String endRegex, String tableName, String charset) throws IOException;

	List<Map<String, Object>> previewTextFile(String data, String datePattern, String dateFormat,
			String dateLocale, String beginRegex, String endRegex, String charset) throws IOException;
	
	void abortTextFile(String guid);
}
