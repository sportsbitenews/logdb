/*
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
package org.araqne.logdb.cep.query;

import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

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
		ConcurrentHashMap<EventKey, EventContext> contexts = new ConcurrentHashMap<EventKey, EventContext>();

		if (rowBatch.selectedInUse) {
			for (int i = 0; i < rowBatch.size; i++) {
				int p = rowBatch.selected[i];
				Row row = rowBatch.rows[p];
				checkEvent(row, new BatchCallback(contexts));
			}
		}else {
			for (int i = 0; i < rowBatch.size; i++) {
				Row row = rowBatch.rows[i];
			
				checkEvent(row, new BatchCallback(contexts));
			}

		}
		storage.addContexts(contexts);
		
		pushPipe(rowBatch);
	}
	
	private void checkEvent(Row row) {
		checkEvent(row, new CallbackAdd() {

			@Override
			public void addJob(EventContext ctx) {
				storage.addContext(ctx); //mem cep 수정 여부에 따라 update->add 방식으로 할지 결정
			}
		});
	}

	private void checkEvent(Row row, CallbackAdd callback) {
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
			if(clockHost != null)
				eventKey.setHost(clockHost);

			long expireTime = 0;
			if (expire != null)
				expireTime = created + expire.unit.getMillis() * expire.amount;

			long timeoutTime = 0;
			if (timeout != null)
				timeoutTime = created + timeout.unit.getMillis() * timeout.amount;

			EventContext	ctx = new EventContext(eventKey, created, expireTime, timeoutTime, maxRows, (String) clockHost);
			ctx.setTimeoutTime(timeoutTime);
			ctx.getCounter().incrementAndGet();
			ctx.addRow(row); 

			callback.addJob(ctx);
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
	
	private interface CallbackAdd{
		
		void addJob(EventContext ctx);
		
	}
	
	private class BatchCallback implements CallbackAdd{
		ConcurrentHashMap<EventKey, EventContext> contexts;

		private BatchCallback(ConcurrentHashMap<EventKey, EventContext> contexts) {
			this.contexts = contexts;
		}
		
		@Override
		public void addJob(EventContext ctx) {
			if(!contexts.contains(ctx)) {
				contexts.put(ctx.getKey(), ctx);
				return;
			}

			EventContext oldCtx = contexts.get(ctx);
			oldCtx.getCounter().incrementAndGet();

			if(ctx.getTimeoutTime()!= 0L)
				oldCtx.setTimeoutTime(ctx.getTimeoutTime());
			
			for(Row row :  ctx.getRows()) {
					oldCtx.addRow(row);
				}
			
			contexts.put(oldCtx.getKey(), oldCtx);
		}
	}
	
}
