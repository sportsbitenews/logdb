/*
 * Copyright 2011 Future Systems
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

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;

public class Rename extends QueryCommand {
	private String from;
	private String to;

	public Rename(String from, String to) {
		this.from = from;
		this.to = to;
	}

	public String getFrom() {
		return from;
	}

	public String getTo() {
		return to;
	}

	@Override
	public void onPush(Row row) {
		Object v = row.remove(from);
		row.put(to, v);
		pushPipe(row);
	}

	@Override
	public void onPush(RowBatch rowBatch) {
		if (rowBatch.selectedInUse) {
			for (int i = 0; i < rowBatch.size; i++) {
				int p = rowBatch.selected[i];
				Row row = rowBatch.rows[p];
				Object v = row.remove(from);
				row.put(to, v);
			}
		} else {
			for (Row row : rowBatch.rows) {
				Object v = row.remove(from);
				row.put(to, v);
			}
		}

		pushPipe(rowBatch);
	}

	@Override
	public String toString() {
		return "rename " + from + " as " + to;
	}
}
