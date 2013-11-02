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

import java.util.Date;

import org.araqne.logdb.LogMap;
import org.araqne.logdb.LogQueryCommand;
import org.araqne.logstorage.Log;
import org.araqne.logstorage.LogStorage;

/**
 * @since 1.6.6
 * @author xeraph
 * 
 */
public class Import extends LogQueryCommand {

	private LogStorage storage;
	private String tableName;

	public Import(LogStorage storage, String tableName) {
		this.storage = storage;
		this.tableName = tableName;
	}

	@Override
	public void push(LogMap m) {
		Object o = m.get("_time");
		Date date = null;
		if (o != null && o instanceof Date)
			date = (Date) o;
		else
			date = new Date();

		storage.write(new Log(tableName, date, m.map()));
		write(m);
	}

	@Override
	public boolean isReducer() {
		return true;
	}

	@Override
	public String toString() {
		return "import " + tableName;
	}
}
