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
import org.araqne.logdb.FunctionRegistry;
import org.araqne.logdb.Row;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.MetadataCallback;
import org.araqne.logdb.MetadataProvider;
import org.araqne.logdb.MetadataService;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogTableRegistry;
import org.araqne.storage.api.FilePath;

@Component(name = "logdb-logdisk-metadata")
public class LogDiskMetadataProvider implements MetadataProvider {

	@Requires
	private LogTableRegistry tableRegistry;

	@Requires
	private AccountService accountService;

	@Requires
	private LogStorage storage;

	@Requires
	private MetadataService metadataService;

	@Requires
	private FunctionRegistry functionRegistry;

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
		return "logdisk";
	}

	@Override
	public void verify(QueryContext context, String queryString) {
		MetadataQueryStringParser.getTableNames(context, tableRegistry, accountService, functionRegistry, queryString);
	}

	@Override
	public void query(QueryContext context, String queryString, MetadataCallback callback) {
		TableScanOption opt = MetadataQueryStringParser.getTableNames(context, tableRegistry, accountService, functionRegistry,
				queryString);
		for (String tableName : opt.getTableNames())
			writeLogDiskUsages(tableName, opt.getFrom(), opt.getTo(), callback);

	}

	private void writeLogDiskUsages(String tableName, Date from, Date to, MetadataCallback callback) {
		FilePath dir = storage.getTableDirectory(tableName);

		FilePath[] files = dir.listFiles();
		if (files == null)
			return;

		List<FilePath> paths = new ArrayList<FilePath>();
		for (FilePath f : files)
			if (f.getName().endsWith(".idx") || f.getName().endsWith(".dat"))
				paths.add(f);

		Collections.sort(paths);

		Date lastDay = null;
		long diskUsage = 0;
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		for (FilePath path : paths) {
			Date day = df.parse(path.getName().substring(0, path.getName().length() - 4), new ParsePosition(0));
			if (day == null)
				continue;

			if (from != null && day.before(from))
				continue;

			if (to != null && day.after(to))
				continue;

			if (lastDay != null && !lastDay.equals(day)) {
				writeDiskUsageLog(tableName, lastDay, diskUsage, callback);
				diskUsage = 0;
			}

			diskUsage += path.length();
			lastDay = day;
		}

		if (lastDay != null)
			writeDiskUsageLog(tableName, lastDay, diskUsage, callback);
	}

	private void writeDiskUsageLog(String tableName, Date day, long diskUsage, MetadataCallback callback) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("_time", day);
		m.put("table", tableName);
		m.put("disk_usage", diskUsage);
		callback.onPush(new Row(m));
	}

}
