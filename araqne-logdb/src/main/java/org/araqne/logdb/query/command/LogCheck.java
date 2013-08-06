package org.araqne.logdb.query.command;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.araqne.logdb.LogMap;
import org.araqne.logdb.LogQueryCommand;
import org.araqne.logstorage.Crypto;
import org.araqne.logstorage.LogFileServiceRegistry;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogTableRegistry;
import org.araqne.logstorage.file.LogBlock;
import org.araqne.logstorage.file.LogBlockCursor;
import org.araqne.logstorage.file.LogFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogCheck extends LogQueryCommand {
	private final Logger logger = LoggerFactory.getLogger(LogCheck.class);

	private Set<String> tableNames;
	private Date from;
	private Date to;

	private LogTableRegistry tableRegistry;
	private LogStorage storage;
	private LogFileServiceRegistry fileServiceRegistry;

	public LogCheck(Set<String> tableNames, Date from, Date to, LogTableRegistry tableRegistry, LogStorage storage,
			LogFileServiceRegistry fileSerivceRegistry) {
		this.tableNames = tableNames;
		this.from = from;
		this.to = to;
		this.tableRegistry = tableRegistry;
		this.storage = storage;
		this.fileServiceRegistry = fileSerivceRegistry;
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
	public void start() {
		try {
			status = Status.Running;
			for (String tableName : tableNames) {
				if (getStatus() == Status.End)
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
		eof(false);
	}

	@Override
	public void push(LogMap m) {
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

			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			String dateText = dateFormat.format(day);

			File indexPath = new File(dir, dateText + ".idx");
			File dataPath = new File(dir, dateText + ".dat");
			File keyPath = new File(dir, dateText + ".key");

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
					write(new LogMap(m));
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
}
