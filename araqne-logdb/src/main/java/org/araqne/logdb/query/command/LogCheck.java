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
package org.araqne.logdb.query.command;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryTask;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowPipe;
import org.araqne.logstorage.Crypto;
import org.araqne.logstorage.LogFileServiceRegistry;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogTableRegistry;
import org.araqne.logstorage.file.LogBlock;
import org.araqne.logstorage.file.LogBlockCursor;
import org.araqne.logstorage.file.LogFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogCheck extends QueryCommand {
	private final Logger logger = LoggerFactory.getLogger(LogCheck.class);

	private IntegrityCheckTask mainTask = new IntegrityCheckTask();
	private Set<String> tableNames;
	private Date from;
	private Date to;

	// for toString query generation
	private String tableToken;

	private LogTableRegistry tableRegistry;
	private LogStorage storage;
	private LogFileServiceRegistry fileServiceRegistry;

	public LogCheck(Set<String> tableNames, Date from, Date to, String tableToken, LogTableRegistry tableRegistry,
			LogStorage storage, LogFileServiceRegistry fileSerivceRegistry) {
		this.tableNames = tableNames;
		this.from = from;
		this.to = to;
		this.tableToken = tableToken;
		this.tableRegistry = tableRegistry;
		this.storage = storage;
		this.fileServiceRegistry = fileSerivceRegistry;
	}

	@Override
	public QueryTask getMainTask() {
		return mainTask;
	}

	public Set<String> getTableNames() {
		return tableNames;
	}

	public Date getFrom() {
		return from;
	}

	public Date getTo() {
		return to;
	}

	@Override
	public void onPush(Row m) {
	}

	@Override
	public boolean isReducer() {
		return false;
	}

	private void checkTable(String tableName) {
		String type = tableRegistry.getTableMetadata(tableName, LogTableRegistry.LogFileTypeKey);

		File dir = storage.getTableDirectory(tableName);

		Map<String, String> tableMetadata = new HashMap<String, String>();
		for (String key : tableRegistry.getTableMetadataKeys(tableName))
			tableMetadata.put(key, tableRegistry.getTableMetadata(tableName, key));

		for (Date day : storage.getLogDates(tableName)) {
			if (getStatus() == Status.End)
				break;

			if (from != null && day.before(from))
				continue;

			if (to != null && day.after(to))
				continue;

			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			String dateText = dateFormat.format(day);

			File indexPath = new File(dir, dateText + ".idx");
			File dataPath = new File(dir, dateText + ".dat");
			File keyPath = new File(dir, dateText + ".key");
			if (!keyPath.exists())
				continue;

			Map<String, Object> options = new HashMap<String, Object>(tableMetadata);
			options.put("tableName", tableName);
			options.put("indexPath", indexPath);
			options.put("dataPath", dataPath);
			options.put("keyPath", keyPath);

			LogFileReader reader = null;
			LogBlockCursor cursor = null;
			try {
				reader = fileServiceRegistry.newReader(tableName, type, options);
				cursor = reader.getBlockCursor();

				while (cursor.hasNext() && getStatus() != Status.End) {
					LogBlock lb = cursor.next();
					Map<String, Object> data = lb.getData();
					Map<String, Object> m = new HashMap<String, Object>();

					byte[] d = (byte[]) data.get("data");
					byte[] signature = (byte[]) data.get("signature");
					if (d != null && signature == null)
						continue;

					String digest = (String) data.get("digest");
					byte[] digestKey = (byte[]) data.get("digest_key");
					byte[] hash = null;
					try {
						if (d != null)
							hash = Crypto.digest(d, d.length, digest, digestKey);
					} catch (Exception e) {
					}

					if (hash != null && Arrays.equals(hash, signature))
						continue;

					m.put("table", tableName);
					m.put("day", day);
					m.put("block_id", data.get("block_id"));
					m.put("signature", signature);
					m.put("hash", hash);
					pushPipe(new Row(m));
				}
			} catch (IOException e) {
				logger.error("araqne logdb: cannot read block metadata", e);
			} finally {
				if (cursor != null) {
					try {
						cursor.close();
					} catch (IOException e) {
					}
				}

				if (reader != null) {
					reader.close();
				}
			}
		}
	}

	@Override
	public String toString() {
		String fromOption = "";
		String toOption = "";

		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
		if (from != null)
			fromOption = " from=" + df.format(from);

		if (to != null)
			toOption = " to=" + df.format(to);

		String tables = tableToken;
		if (!tables.isEmpty())
			tables = " " + tables;

		return "logcheck" + fromOption + toOption + tables;
	}

	private class IntegrityCheckTask extends QueryTask {
		@Override
		public void run() {
			try {
				for (String tableName : tableNames) {
					if (getStatus() == TaskStatus.CANCELED)
						break;

					try {
						checkTable(tableName);
					} catch (UnsupportedOperationException e) {
						// ignore unsupported reader
					}
				}
			} catch (Throwable t) {
				logger.error("araqne logdb: table error", t);
			}
		}

		@Override
		public RowPipe getOutput() {
			return output;
		}
	}

}
