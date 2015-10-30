/*
 * Copyright 2014 Eediom Inc.
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
package org.araqne.logdb.cep.query;

import java.util.Date;
import java.util.concurrent.CopyOnWriteArrayList;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.ThreadSafe;
import org.araqne.logdb.cep.EventCause;
import org.araqne.logdb.cep.EventContextStorage;
import org.araqne.logdb.cep.EventKey;
import org.araqne.logdb.query.expr.Expression;

public class EvtCtxDelCommand extends QueryCommand implements ThreadSafe {
	private EventContextStorage storage;
	private String topic;
	private String keyField;
	private Expression matcher;

	// host field for external clock
	private String hostField;

	public EvtCtxDelCommand(EventContextStorage storage, String topic, String keyField, Expression matcher, String hostField) {
		this.storage = storage;
		this.topic = topic;
		this.keyField = keyField;
		this.matcher = matcher;
		this.hostField = hostField;
	}

	@Override
	public String getName() {
		return "evtctxdel";
	}

	@Override
	public void onPush(Row row) {
		EventKey key = buildEventKey(row);
		if (key != null)
			storage.removeContext(key, EventCause.REMOVAL);

		pushPipe(row);
	}

	@Override
	public void onPush(RowBatch rowBatch) {
		CopyOnWriteArrayList<EventKey> batchContexts = new CopyOnWriteArrayList<EventKey>();

		if (rowBatch.selectedInUse) {
			for (int i = 0; i < rowBatch.size; i++) {
				int p = rowBatch.selected[i];
				Row row = rowBatch.rows[p];
				EventKey key = buildEventKey(row);
				if (key != null)
					batchContexts.add(key);
			}
		} else {
			for (int i = 0; i < rowBatch.size; i++) {
				Row row = rowBatch.rows[i];
				EventKey key = buildEventKey(row);
				if (key != null)
					batchContexts.add(key);
			}
		}

		storage.removeContexts(batchContexts, EventCause.REMOVAL);
		pushPipe(rowBatch);
	}

	private EventKey buildEventKey(Row row) {
		boolean matched = true;

		Object o = matcher.eval(row);
		if (o == null)
			matched = false;

		if (o instanceof Boolean && !(Boolean) o)
			matched = false;

		// extract host for log tick
		String host = null;
		Object h = null;

		if (hostField != null)
			h = row.get(hostField);

		if (h != null)
			host = h.toString();

		Object k = row.get(keyField);
		if (k == null)
			matched = false;

		// extract log time
		Date logTime = null;
		Object t = row.get("_time");
		if (t instanceof Date)
			logTime = (Date) t;

		if (host != null && logTime != null)
			storage.advanceTime(host, logTime.getTime());

		if (matched) {
			String key = k.toString();
			EventKey eventKey = new EventKey(topic, key, host);
			return eventKey;
		}
		return null;
	}

	@Override
	public String toString() {
		return "evtctxdel topic=" + topic + " key=" + keyField + " " + matcher;
	}
}
