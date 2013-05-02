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
import java.util.List;
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
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogTableRegistry;

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
	public void verify(LogQueryContext context, String queryString) {
	}

	@Override
	public void query(LogQueryContext context, String queryString, MetadataCallback callback) {
		for (String tableName : MetadataQueryStringParser.getTableNames(context, tableRegistry, accountService, queryString))
			writeLogDiskUsages(tableName, callback);

	}

	private void writeLogDiskUsages(String tableName, MetadataCallback callback) {
		File dir = storage.getTableDirectory(tableName);

		File[] files = dir.listFiles();
		if (files == null)
			return;

		List<String> paths = new ArrayList<String>();
		for (File f : files)
			if (f.getName().endsWith(".idx") || f.getName().endsWith(".dat"))
				paths.add(f.getAbsolutePath());

		Collections.sort(paths);

		Date lastDay = null;
		long diskUsage = 0;
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		for (String path : paths) {
			File f = new File(path);
			Date day = df.parse(f.getName().substring(0, f.getName().length() - 4), new ParsePosition(0));
			if (day == null)
				continue;

			if (lastDay != null && !lastDay.equals(day)) {
				writeDiskUsageLog(tableName, lastDay, diskUsage, callback);
				diskUsage = 0;
			}

			diskUsage += f.length();
			lastDay = day;
		}

		writeDiskUsageLog(tableName, lastDay, diskUsage, callback);
	}

	private void writeDiskUsageLog(String tableName, Date day, long diskUsage, MetadataCallback callback) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("_time", day);
		m.put("table", tableName);
		m.put("disk_usage", diskUsage);
		callback.onLog(new LogMap(m));
	}

}