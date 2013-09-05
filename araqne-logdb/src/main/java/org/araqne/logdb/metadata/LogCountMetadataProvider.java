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

import java.io.File;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logdb.AccountService;
import org.araqne.logdb.LogMap;
import org.araqne.logdb.LogQueryContext;
import org.araqne.logdb.MetadataCallback;
import org.araqne.logdb.MetadataProvider;
import org.araqne.logdb.MetadataService;
import org.araqne.logstorage.LogFileService;
import org.araqne.logstorage.LogFileServiceRegistry;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogTableRegistry;

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
	public void verify(LogQueryContext context, String queryString) {
		MetadataQueryStringParser.getTableNames(context, tableRegistry, accountService, queryString);
	}

	@Override
	public void query(LogQueryContext context, String queryString, MetadataCallback callback) {
		TableScanOption opt = MetadataQueryStringParser.getTableNames(context, tableRegistry, accountService, queryString);
		for (String tableName : opt.getTableNames())
			countFiles(tableName, opt.getFrom(), opt.getTo(), callback);
	}

	private void countFiles(String tableName, Date from, Date to, MetadataCallback callback) {
		String fileType = tableRegistry.getTableMetadata(tableName, "_filetype");
		if (fileType == null)
			fileType = "v2";

		File dir = storage.getTableDirectory(tableName);
		countFiles(tableName, fileType, dir, from, to, callback);
	}

	private void countFiles(String tableName, String type, File dir, Date from, Date to, MetadataCallback callback) {
		File[] files = dir.listFiles();
		if (files == null)
			return;

		LogFileService fileService = logFileServiceRegistry.getLogFileService(type);
		if (fileService == null)
			return;

		ArrayList<String> paths = new ArrayList<String>();
		for (File f : files)
			if (f.isFile())
				paths.add(f.getAbsolutePath());

		Collections.sort(paths);

		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		for (String path : paths) {
			File f = new File(path);
			if (f.getName().endsWith(".idx")) {
				long count = fileService.count(f);
				Date day = df.parse(f.getName().substring(0, f.getName().length() - 4), new ParsePosition(0));
				if (day == null)
					continue;

				if (from != null && day.before(from))
					continue;

				if (to != null && day.after(to))
					continue;

				writeCount(tableName, day, count, callback);
			}
		}
	}

	private void writeCount(String tableName, Date day, long count, MetadataCallback callback) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("_time", day);
		m.put("table", tableName);
		m.put("count", count);
		callback.onLog(new LogMap(m));
	}
}
