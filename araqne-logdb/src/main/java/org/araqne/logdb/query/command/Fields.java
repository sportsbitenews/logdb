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

import java.util.List;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.Strings;
import org.araqne.logdb.ThreadSafe;

public class Fields extends QueryCommand implements ThreadSafe {
	private final List<String> fields;
	private final boolean selector;

	public Fields(List<String> fields) {
		this(fields, true);
	}

	public Fields(List<String> fields, boolean selector) {
		this.fields = fields;
		this.selector = selector;
	}

	@Override
	public String getName() {
		return "fields";
	}

	@Override
	public void onPush(Row m) {
		if (selector) {
			Row newMap = new Row();
			for (String field : fields) {
				Object data = m.get(field);
				newMap.put(field, data);
			}
			m = newMap;
		} else {
			for (String field : fields)
				m.remove(field);
		}
		pushPipe(m);
	}

	@Override
	public void onPush(RowBatch rowBatch) {
		if (rowBatch.selectedInUse) {
			for (int i = 0; i < rowBatch.size; i++) {
				int p = rowBatch.selected[i];
				Row row = rowBatch.rows[p];

				if (selector) {
					Row newMap = new Row();
					for (String field : fields) {
						Object data = row.get(field);
						newMap.put(field, data);
					}
					rowBatch.rows[p] = newMap;
				} else {
					for (String field : fields)
						row.remove(field);
				}
			}
		} else {
			for (int i = 0; i < rowBatch.size; i++) {
				Row row = rowBatch.rows[i];

				if (selector) {
					Row newMap = new Row();
					for (String field : fields) {
						Object data = row.get(field);
						newMap.put(field, data);
					}
					rowBatch.rows[i] = newMap;
				} else {
					for (String field : fields)
						row.remove(field);
				}
			}
		}

		pushPipe(rowBatch);
	}

	public List<String> getFields() {
		return fields;
	}

	public boolean isSelector() {
		return selector;
	}

	@Override
	public String toString() {
		String removeOption = "";
		if (!selector)
			removeOption = "- ";

		return "fields " + removeOption + Strings.join(fields, ", ");
	}

}
