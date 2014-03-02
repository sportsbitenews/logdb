/*
 * Copyright 2013 Eediom Inc.
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
package org.araqne.logdb.metadata;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logdb.AccountService;
import org.araqne.logdb.MetadataCallback;
import org.araqne.logdb.MetadataProvider;
import org.araqne.logdb.MetadataService;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logstorage.LogFileService;
import org.araqne.logstorage.LogFileServiceRegistry;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogTableRegistry;
import org.araqne.logstorage.LogWriterStatus;
import org.araqne.logstorage.TableSchema;
import org.araqne.storage.api.FilePath;

@Component(name = "logdb-logcount-metadata")
public class LogCountMetadataProvider implements MetadataProvider {
	@Requires
	private AccountService accountService;

	@Requires
	private LogTableRegistry tableRegistry;

	@Requires
	private LogStorage storage;

	@Requires
	private LogFileServiceRegistry logFileServiceRegistry;

	@Requires
	private MetadataService metadataService;

	@Validate
	public void start() {
		metadataService.addProvider(this);
	}

	@Invalidate
	public void stop() {
		if (metadataService != null)
			metadataService.removeProvider(this);
	}

	@Override
	public String getType() {
		return "count";
	}

	@Override
	public void verify(QueryContext context, String queryString) {
		MetadataQueryStringParser.getTableNames(context, tableRegistry, accountService, queryString);
	}

	@Override
	public void query(QueryContext context, String queryString, MetadataCallback callback) {
		TableScanOption opt = MetadataQueryStringParser.getTableNames(context, tableRegistry, accountService, queryString);
		List<LogWriterStatus> memoryBuffers = new ArrayList<LogWriterStatus>();
		if (!opt.isDiskOnly())
			memoryBuffers = storage.getWriterStatuses();

		for (String tableName : opt.getTableNames())
			countFiles(tableName, opt.getFrom(), opt.getTo(), memoryBuffers, callback);
	}

	private int getMemoryCount(List<LogWriterStatus> memoryBuffers, String tableName, Date day) {
		for (LogWriterStatus buffer : memoryBuffers)
			if (buffer.getTableName().equals(tableName) && buffer.getDay().equals(day))
				return buffer.getBufferSize();

		return 0;
	}

	private void countFiles(String tableName, Date from, Date to, List<LogWriterStatus> memoryBuffers, MetadataCallback callback) {
		TableSchema schema = tableRegistry.getTableSchema(tableName, true);
		String fileType = schema.getPrimaryStorage().getType();
		FilePath dir = storage.getTableDirectory(tableName);
		countFiles(tableName, fileType, dir, from, to, memoryBuffers, callback);
	}

	private void countFiles(String tableName, String type, FilePath dir, Date from, Date to, List<LogWriterStatus> memoryBuffers,
			MetadataCallback callback) {
		FilePath[] files = dir.listFiles();
		if (files == null)
			return;

		LogFileService fileService = logFileServiceRegistry.getLogFileService(type);
		if (fileService == null)
			return;

		ArrayList<FilePath> paths = new ArrayList<FilePath>();
		for (FilePath f : files)
			if (f.isFile())
				paths.add(f);

		Collections.sort(paths);

		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		for (FilePath path : paths) {
			if (path.getName().endsWith(".idx")) {
				long count = fileService.count(path);
				Date day = df.parse(path.getName().substring(0, path.getName().length() - 4), new ParsePosition(0));
				if (day == null)
					continue;

				if (from != null && day.before(from))
					continue;

				if (to != null && day.after(to))
					continue;

				count += getMemoryCount(memoryBuffers, tableName, day);
				writeCount(tableName, day, count, callback);
			}
		}
	}

	private void writeCount(String tableName, Date day, long count, MetadataCallback callback) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("_time", day);
		m.put("table", tableName);
		m.put("count", count);
		callback.onPush(new Row(m));
	}
}
