package org.araqne.logdb.query.command;

import java.util.Date;
import java.util.Map;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.Row;
import org.araqne.logdb.ThreadSafe;
import org.araqne.logstorage.Log;
import org.araqne.logstorage.LogStorage;

public class Insert extends QueryCommand implements ThreadSafe {
	private final LogStorage storage;
	private String tableNameField;
	private boolean create;

	public Insert(LogStorage storage, String tableNameField, boolean create) {
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
			try {
				storage.ensureTable(tableName, "v3p");
			} catch (Throwable t) {
			}

			try {
				storage.ensureTable(tableName, "v2");
			} catch (Throwable t) {
			}
		}

		Map<String, Object> log = row.clone().map();
		log.remove(tableNameField);
		Object o = log.get("_time");
		Date date = null;
		if (o != null && o instanceof Date)
			date = (Date) o;
		else
			date = new Date();

		storage.write(new Log(tableName, date, log));
		pushPipe(row);
	}

	@Override
	public String toString() {
		String createOption = "";
		if (create)
			createOption = "create=true ";

		return "insert into=" + tableNameField + " " + createOption;
	}
}
