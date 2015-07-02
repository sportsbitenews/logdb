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
package org.araqne.logstorage.engine;

import java.io.IOException;
import java.util.Date;

import org.araqne.logstorage.LogFileServiceRegistry;
import org.araqne.logstorage.LogPartitionNotFoundException;
import org.araqne.logstorage.LogTableRegistry;
import org.araqne.logstorage.TableSchema;
import org.araqne.logstorage.file.DatapathUtil;
import org.araqne.logstorage.file.LogFileReader;
import org.araqne.logstorage.file.LogFileServiceV2;
import org.araqne.storage.api.FilePath;
import org.araqne.storage.api.StorageManager;

/**
 * 
 * @author xeraph
 * @since 0.9
 */
class LogFileFetcher {
	private final LogTableRegistry tableRegistry;
	private final LogFileServiceRegistry lfsRegistry;
	private final StorageManager storageManager;

	public LogFileFetcher(LogTableRegistry tableRegistry, LogFileServiceRegistry lfsRegistry, StorageManager storageManager) {
		this.tableRegistry = tableRegistry;
		this.lfsRegistry = lfsRegistry;
		this.storageManager = storageManager;
	}

	public LogFileReader fetch(String tableName, Date day, FilePath logDir) throws IOException {
		// FIXME : add option for path
		TableSchema schema = tableRegistry.getTableSchema(tableName, true);
		int tableId = schema.getId();
		String basePathString = schema.getPrimaryStorage().getBasePath();
		FilePath basePath = logDir;
		if (basePathString != null)
			basePath = storageManager.resolveFilePath(basePathString);

		FilePath indexPath = DatapathUtil.getIndexFile(tableId, day, basePath);
		if (!indexPath.exists())
			throw new LogPartitionNotFoundException("log table not found: " + tableName + ", " + day);

		FilePath dataPath = DatapathUtil.getDataFile(tableId, day, basePath);
		if (!dataPath.exists())
			throw new LogPartitionNotFoundException("log table not found: " + tableName + ", " + day);

		FilePath keyPath = DatapathUtil.getKeyFile(tableId, day, basePath);

		String logFileType = schema.getPrimaryStorage().getType();
		LogFileServiceV2.Option options = new LogFileServiceV2.Option(schema.getPrimaryStorage(), schema.getMetadata(),
				tableName, basePath, indexPath, dataPath, keyPath);
		options.put("day", day);
		return lfsRegistry.newReader(tableName, logFileType, options);

	}
}
