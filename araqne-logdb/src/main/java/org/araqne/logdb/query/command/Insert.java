package org.araqne.logdb.query.command;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.ThreadSafe;
import org.araqne.logstorage.Log;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogTableRegistry;
import org.araqne.logstorage.StorageConfig;
import org.araqne.logstorage.TableSchema;

public class Insert extends QueryCommand implements ThreadSafe {
	private final LogTableRegistry tableRegistry;
	private final LogStorage storage;
	private final boolean create;
	private final String tableNameField;

	public Insert(LogTableRegistry tableRegistry, LogStorage storage, String tableNameField, boolean create) {
		this.tableRegistry = tableRegistry;
		this.storage = storage;
		this.tableNameField = tableNameField;
		this.create = create;
	}

	@Override
	public String getName() {
		return "insert";
	}

	@Override
	public void onPush(Row row) {
		Map<String, Object> original = row.map();
		String tableName = original.get(tableNameField) == null ? null : original.get(tableNameField).toString();
		if (tableName == null || tableName.isEmpty())
			return;

		if (create) {
			createTable(tableName);
		}

		Map<String, Object> log = row.clone().map();
		log.remove(tableNameField);
		Object o = log.get("_time");
		Date date = null;
		if (o != null && o instanceof Date)
			date = (Date) o;
		else
			date = new Date();

		try {
			storage.write(new Log(tableName, date, log));
			pushPipe(row);
		} catch (InterruptedException e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	public void onPush(RowBatch rowBatch) {
		HashMap<String, List<Log>> tableLogs = new HashMap<String, List<Log>>();

		if (rowBatch.selectedInUse) {
			for (int i = 0; i < rowBatch.size; i++) {
				int p = rowBatch.selected[i];
				Row row = rowBatch.rows[p];
				addLog(tableLogs, row);
			}
		} else {
			for (int i = 0; i < rowBatch.size; i++) {
				Row row = rowBatch.rows[i];
				addLog(tableLogs, row);
			}
		}

		try {
			if (create) {
				for (String tableName : tableLogs.keySet()) {
					if (tableRegistry.exists(tableName))
						continue;
					
					createTable(tableName);
				}
			}

			for (String key : tableLogs.keySet()) {
				List<Log> logs = tableLogs.get(key);
				storage.write(logs);
			}

		} catch (InterruptedException e) {
			throw new IllegalStateException(e);
		}

		pushPipe(rowBatch);
	}

	private void createTable(String tableName) {
		try {
			storage.ensureTable(new TableSchema(tableName, new StorageConfig("v3p")));
		} catch (Throwable t) {
		}

		try {
			storage.ensureTable(new TableSchema(tableName, new StorageConfig("v2")));
		} catch (Throwable t) {
		}
	}

	private void addLog(Map<String, List<Log>> tableLogs, Row row) {
		String tableName = row.get(tableNameField) == null ? null : row.get(tableNameField).toString();
		if (tableName == null || tableName.isEmpty())
			return;

		Map<String, Object> m = Row.clone(row.map());
		m.remove(tableNameField);

		Object o = m.get("_time");
		Date date = null;
		if (o != null && o instanceof Date)
			date = (Date) o;
		else
			date = new Date();

		List<Log> logs = tableLogs.get(tableName);
		if (logs == null) {
			logs = new ArrayList<Log>();
			tableLogs.put(tableName, logs);
		}

		logs.add(new Log(tableName, date, m));
	}

	@Override
	public String toString() {
		String createOption = "";
		if (create)
			createOption = "create=t ";

		return "insert into=" + tableNameField + " " + createOption;
	}
}
