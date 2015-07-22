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
import org.araqne.logstorage.Crypto;
import org.araqne.logstorage.LogFileServiceRegistry;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogTableRegistry;
import org.araqne.logstorage.TableSchema;
import org.araqne.logstorage.file.LogBlock;
import org.araqne.logstorage.file.LogBlockCursor;
import org.araqne.logstorage.file.LogFileReader;
import org.araqne.logstorage.file.LogFileServiceV2;
import org.araqne.storage.api.FilePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckTable extends QueryCommand {
	private final Logger logger = LoggerFactory.getLogger(CheckTable.class);

	private final String commandName;
	private IntegrityCheckTask mainTask = new IntegrityCheckTask();
	private Set<String> tableNames;
	private Date from;
	private Date to;
	private boolean trace;

	// for toString query generation
	private String tableToken;

	private LogTableRegistry tableRegistry;
	private LogStorage storage;
	private LogFileServiceRegistry fileServiceRegistry;

	public CheckTable(String commandName, Set<String> tableNames, Date from, Date to, boolean trace, String tableToken,
			LogTableRegistry tableRegistry, LogStorage storage, LogFileServiceRegistry fileSerivceRegistry) {
		this.commandName = commandName;
		this.tableNames = tableNames;
		this.from = from;
		this.to = to;
		this.trace = trace;
		this.tableToken = tableToken;
		this.tableRegistry = tableRegistry;
		this.storage = storage;
		this.fileServiceRegistry = fileSerivceRegistry;
	}

	@Override
	public String getName() {
		return "checktable";
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
		TableSchema schema = tableRegistry.getTableSchema(tableName, true);
		Map<String, String> metadata = schema.getMetadata();
		String type = schema.getPrimaryStorage().getType();

		FilePath dir = storage.getTableDirectory(tableName);

		for (Date day : storage.getLogDates(tableName)) {
			if (getStatus() == Status.End)
				break;

			if (from != null && day.before(from))
				continue;

			if (to != null && day.after(to))
				continue;

			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			String dateText = dateFormat.format(day);

			FilePath indexPath = dir.newFilePath(dateText + ".idx");
			FilePath dataPath = dir.newFilePath(dateText + ".dat");
			FilePath keyPath = dir.newFilePath(dateText + ".key");
			if (!keyPath.exists())
				continue;

			LogFileReader reader = null;
			LogBlockCursor cursor = null;
			int lastValidBlockId = 0;
			try {
				reader = fileServiceRegistry.newReader(tableName, type, new LogFileServiceV2.Option(schema.getPrimaryStorage(),
						metadata, tableName, dir, indexPath, dataPath, keyPath));
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

					lastValidBlockId = (Integer) data.get("block_id");

					boolean valid = hash != null && Arrays.equals(hash, signature);
					if (valid && !trace)
						continue;

					m.put("table", tableName);
					m.put("day", day);
					m.put("block_id", data.get("block_id"));
					m.put("signature", signature);
					m.put("hash", hash);

					if (d == null)
						m.put("msg", "corrupted");
					else
						m.put("msg", valid ? "valid" : "modified");
					pushPipe(new Row(m));
				}
			} catch (IOException e) {
				Map<String, Object> m = new HashMap<String, Object>();
				m.put("table", tableName);
				m.put("day", day);
				m.put("last_block_id", lastValidBlockId);
				m.put("msg", "corrupted");
				pushPipe(new Row(m));
				logger.trace("araqne logdb: cannot read block metadata", e);
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
		String traceOption = "";

		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
		if (from != null)
			fromOption = " from=" + df.format(from);

		if (to != null)
			toOption = " to=" + df.format(to);

		if (trace)
			traceOption = " trace=t";

		String tables = tableToken;
		if (!tables.isEmpty())
			tables = " " + tables;

		return commandName + fromOption + toOption + traceOption + tables;
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
	}
}
