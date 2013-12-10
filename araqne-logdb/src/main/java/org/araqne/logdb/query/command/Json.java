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

import java.util.List;

import org.araqne.logdb.DriverQueryCommand;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;

public class Json extends DriverQueryCommand {
	private List<Row> logs;

	// original json string for toString convenience
	private String json;

	public Json(List<Row> logs, String json) {
		this.logs = logs;
		this.json = json;
	}

	@Override
	public void run() {
		RowBatch rowBatch = new RowBatch();
		rowBatch.size = logs.size();
		rowBatch.rows = logs.toArray(new Row[0]);
		pushPipe(rowBatch);
	}

	public List<Row> getLogs() {
		return logs;
	}

	public String getJson() {
		return json;
	}

	@Override
	public String toString() {
		return "json " + json;
	}
}
