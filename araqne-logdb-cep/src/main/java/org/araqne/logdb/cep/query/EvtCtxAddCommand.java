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

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.ThreadSafe;
import org.araqne.logdb.TimeSpan;
import org.araqne.logdb.cep.EventContext;
import org.araqne.logdb.cep.EventContextStorage;
import org.araqne.logdb.cep.EventKey;
import org.araqne.logdb.query.expr.Expression;

public class EvtCtxAddCommand extends QueryCommand implements ThreadSafe {

	private EventContextStorage storage;
	private String topic;
	private String keyField;
	private TimeSpan expire;
	private TimeSpan timeout;
	private int maxRows;
	private Expression matcher;

	// host field for external clock
	private String hostField;

	public EvtCtxAddCommand(EventContextStorage storage, String topic, String keyField, TimeSpan expire, TimeSpan timeout,
			int maxRows, Expression matcher, String hostField) {
		this.storage = storage;
		this.topic = topic;
		this.keyField = keyField;
		this.expire = expire;
		this.timeout = timeout;
		this.maxRows = maxRows;
		this.matcher = matcher;
		this.hostField = hostField;
	}

	@Override
	public String getName() {
		return "evtctxadd";
	}

	@Override
	public void onPush(Row row) {
		checkEvent(row);
		pushPipe(row);
	}

	@Override
	public void onPush(RowBatch rowBatch) {
		if (rowBatch.selectedInUse) {
			for (int i = 0; i < rowBatch.size; i++) {
				int p = rowBatch.selected[i];
				Row row = rowBatch.rows[p];
				checkEvent(row);
			}
		} else {
			for (int i = 0; i < rowBatch.size; i++) {
				Row row = rowBatch.rows[i];
				checkEvent(row);
			}
		}

		pushPipe(rowBatch);
	}

	private void checkEvent(Row row) {
		boolean matched = true;

		Object o = matcher.eval(row);
		if (o == null)
			matched = false;

		if (o instanceof Boolean && !(Boolean) o)
			matched = false;

		Object k = row.get(keyField);
		if (k == null)
			matched = false;

		// extract host for log tick
		String clockHost = null;
		Object h = null;

		if (hostField != null)
			h = row.get(hostField);

		if (h != null)
			clockHost = h.toString();

		// extract log time
		long created = 0;
		Date logTime = null;
		Object t = row.get("_time");
		if (clockHost != null && t instanceof Date) {
			logTime = (Date) t;
			created = logTime.getTime();
		} else {
			created = System.currentTimeMillis();
		}

		if (matched) {
			String key = k.toString();
			EventKey eventKey = new EventKey(topic, key);

			long expireTime = 0;
			if (expire != null)
				expireTime = created + expire.unit.getMillis() * expire.amount;

			long timeoutTime = 0;
			if (timeout != null)
				timeoutTime = created + timeout.unit.getMillis() * timeout.amount;

			boolean newContext = false;
			EventContext ctx = storage.getContext(eventKey);
			if (ctx == null) {
				ctx = new EventContext(eventKey, created, expireTime, timeoutTime, maxRows, (String) clockHost);
				EventContext oldCtx = storage.addContext(ctx);
				newContext = ctx == oldCtx;
				ctx = oldCtx;
			}

			ctx.getCounter().incrementAndGet();

			// extend timeout
			if (!newContext)
				ctx.setTimeoutTime(timeoutTime);

			ctx.addRow(row);
		}

		if (clockHost != null && logTime != null) {
			Object date = row.get("_time");
			if (date instanceof Date)
				storage.advanceTime(clockHost, logTime.getTime());
		}
	}

	@Override
	public String toString() {
		String s = "evtctxadd topic=" + topic + " key=" + keyField;

		if (expire != null)
			s += " expire=" + expire;

		if (timeout != null)
			s += " timeout=" + timeout;

		if (maxRows != 10)
			s += " maxrows=" + maxRows;

		if (hostField != null)
			s += " logtick=" + hostField;

		s += " " + matcher;

		return s;
	}
}
