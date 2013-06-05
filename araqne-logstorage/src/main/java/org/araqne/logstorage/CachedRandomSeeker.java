/*
 * Copyright 2013 Future Systems
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.araqne.logstorage;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import org.araqne.logstorage.file.LogRecord;

/**
 * cache opened reader while index seek (to prevent repetitive file open)
 * 
 * @author xeraph
 * @since 0.9
 */
public interface CachedRandomSeeker {
	Log getLog(String tableName, Date day, long id) throws IOException;
	LogRecord getLogRecord(String tableName, Date day, long id) throws IOException;
	List<LogRecord> getLogRecords(String tableName, Date day, List<Long> ids);

	void close();
}
