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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.ThreadSafe;
import org.araqne.logstorage.Log;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.StorageConfig;
import org.araqne.logstorage.TableSchema;

/**
 * @since 1.6.6
 * @author xeraph
 * 
 */
public class Import extends QueryCommand implements ThreadSafe {
	private final LogStorage storage;
	private final String tableName;

	/**
	 * @since 1.8.2
	 */
	private final boolean create;

	public Import(LogStorage storage, String tableName, boolean create) {
		this.storage = storage;
		this.tableName = tableName;
		this.create = create;
	}

	@Override
	public String getName() {
		return "import";
	}

	@Override
	public void onStart() {
		if (create) {
			try {
				storage.ensureTable(new TableSchema(tableName, new StorageConfig("v3p")));
			} catch (Throwable t) {
			}

			try {
				storage.ensureTable(new TableSchema(tableName, new StorageConfig("v2")));
			} catch (Throwable t) {
			}
		}
	}

	public String getTableName() {
		return tableName;
	}

	public boolean isCreate() {
		return create;
	}

	@Override
	public void onPush(Row row) {
		Log log = convertToLog(row);

		try {
			storage.write(log);
			pushPipe(row);
		} catch (InterruptedException e) {
			getQuery().cancel(QueryStopReason.Interrupted);
		}
	}

	@Override
	public void onPush(RowBatch rowBatch) {
		List<Log> logs = new ArrayList<Log>(rowBatch.size);
		if (rowBatch.selectedInUse) {
			for (int i = 0; i < rowBatch.size; i++) {
				int p = rowBatch.selected[i];
				Row row = rowBatch.rows[p];
				logs.add(convertToLog(row));
			}
		} else {
			for (int i = 0; i < rowBatch.size; i++) {
				Row row = rowBatch.rows[i];
				logs.add(convertToLog(row));
			}
		}

		try {
			storage.write(logs);
			pushPipe(rowBatch);
		} catch (InterruptedException e) {
			getQuery().cancel(QueryStopReason.Interrupted);
		}
	}

	private Log convertToLog(Row row) {
		Object o = row.get("_time");
		Date date = null;
		if (o != null && o instanceof Date)
			date = (Date) o;
		else
			date = new Date();

		Log log = new Log(tableName, date, Row.clone(row.map()));
		return log;
	}

	@Override
	public boolean isReducer() {
		return true;
	}

	@Override
	public String toString() {
		String createOption = "";
		if (create)
			createOption = "create=t ";

		return "import " + createOption + tableName;
	}
}
