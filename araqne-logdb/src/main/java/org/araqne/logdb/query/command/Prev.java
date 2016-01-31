/**
 * Copyright 2015 Eediom Inc.
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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.ThreadSafe;

public class Prev extends QueryCommand implements ThreadSafe {

	private Object lock = new Object();
	private String[] inFields;
	private String[] outFields;
	private Object[] oldValues;

	public Prev(String[] inFields) {
		this.inFields = inFields;
		this.outFields = new String[inFields.length];
		this.oldValues = new Object[inFields.length];
		for (int i = 0; i < inFields.length; i++)
			this.outFields[i] = "prev_" + inFields[i];
	}

	@Override
	public String getName() {
		return "prev";
	}

	@Override
	public void onPush(Row row) {
		synchronized (lock) {
			addPrevFields(row);
		}

		pushPipe(row);
	}

	@Override
	public void onPush(RowBatch rowBatch) {
		synchronized (lock) {
			if (rowBatch.selectedInUse) {
				for (int i = 0; i < rowBatch.size; i++) {
					int p = rowBatch.selected[i];
					Row row = rowBatch.rows[p];
					addPrevFields(row);
				}
			} else {
				for (int i = 0; i < rowBatch.size; i++) {
					Row row = rowBatch.rows[i];
					addPrevFields(row);
				}
			}
		}

		pushPipe(rowBatch);
	}

	private void addPrevFields(Row row) {
		for (int i = 0; i < inFields.length; i++) {
			String inField = inFields[i];
			String outField = outFields[i];
			row.put(outField, copy(oldValues[i]));
			oldValues[i] = copy(row.get(inField));
		}
	}

	@SuppressWarnings("unchecked")
	private Object copy(Object o) {
		if (o == null)
			return null;

		// return immutable object as is, otherwise, clone.
		if (o instanceof String) {
			return o;
		} else if (o instanceof Number) {
			return o;
		} else if (o instanceof Date) {
			return ((Date) o).clone();
		} else if (o instanceof InetAddress) {
			return o;
		} else if (o instanceof Map) {
			Map<String, Object> m = (Map<String, Object>) o;
			Map<String, Object> n = new HashMap<String, Object>();
			for (String key : m.keySet()) {
				n.put(key, copy(m.get(key)));
			}
			return n;
		} else if (o instanceof Collection) {
			List<Object> n = new ArrayList<Object>();
			for (Object c : (Collection<?>) o) {
				n.add(copy(c));
			}
			return n;
		} else if (o instanceof Boolean) {
			return o;
		} else {
			return o;
		}
	}
}
