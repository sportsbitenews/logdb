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
package org.araqne.logdb.cep;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;

import org.araqne.logdb.Row;
import org.araqne.msgbus.Marshalable;

public class EventContext implements Marshalable{
	private EventKey key;
	private List<Row> rows;

	private AtomicInteger counter = new AtomicInteger();

	// log time or real time
	private long created;

	// 0 means infinite, absolutely disappears
	private long expireTime;

	// 0 means infinite, extended when new row arrives
	private long timeoutTime;

	private int maxRows;

	// host for external log tick
	private String host;

	private HashMap<String, Object> variables = new HashMap<String, Object>(1);

	private CopyOnWriteArraySet<EventContextListener> listeners = new CopyOnWriteArraySet<EventContextListener>();

	public EventContext(EventKey key, long created, long expireTime, long timeoutTime, int maxRows, String host) {
		this.key = key;
		this.created = created;
		this.rows = Collections.synchronizedList(new ArrayList<Row>());
		this.expireTime = expireTime;
		this.timeoutTime = timeoutTime;
		this.maxRows = maxRows;
		this.host = host;
	}

	public EventKey getKey() {
		return key;
	}

	public void setKey(EventKey key) {
		this.key = key;
	}

	public List<Row> getRows() {
		return new ArrayList<Row>(rows);
	}

	public long getTimeoutTime() {
		return timeoutTime;
	}

	public void setTimeoutTime(long timeoutTime) {
		this.timeoutTime = timeoutTime;

		for (EventContextListener listener : listeners) {
			listener.onUpdateTimeout(this);
		}
	}

	public long getExpireTime() {
		return expireTime;
	}

	public void setExpireTime(long expireTime) {
		this.expireTime = expireTime;
	}

	public AtomicInteger getCounter() {
		return counter;
	}

	public int getMaxRows() {
		return maxRows;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public long getCreated() {
		return created;
	}

	public void setCreated(long created) {
		this.created = created;
	}

	public void addRow(Row row) {
		synchronized (rows) {
			if (rows.size() < maxRows) {
				// deep copy is required. passed row can be modified later.
				row = new Row(Row.clone(row.map()));
				rows.add(row);
			}
		}
	}

	public void removeRow(Row row) {
		synchronized (rows) {
			rows.remove(row);
		}
	}

	public Map<String, Object> getVariables() {
		return Row.clone(variables);
	}

	@SuppressWarnings("unchecked")
	public Object getVariable(String key) {
		synchronized (variables) {
			Object o = variables.get(key);

			// prevent input data corruption from outside
			if (o instanceof Collection)
				return Row.clone((Collection<Object>) o);
			if (o instanceof Map)
				return Row.clone((Map<String, Object>) o);

			return o;
		}
	}

	@SuppressWarnings("unchecked")
	public void setVariable(String key, Object value) {
		// prevent input data corruption from outside
		if (value instanceof Collection)
			value = Row.clone((Collection<Object>) value);
		if (value instanceof Map)
			value = Row.clone((Map<String, Object>) value);

		synchronized (variables) {
			variables.put(key, value);
		}
	}

	public CopyOnWriteArraySet<EventContextListener> getListeners() {
		return listeners;
	}

	public void setListeners(CopyOnWriteArraySet<EventContextListener> listeners) {
		this.listeners = listeners;
	}

	private List<Map<String, Object>> format(List<Row> rows) {
		List<Map<String, Object>> rowMaps = new ArrayList<Map<String, Object>>();

		for(Row row : rows) {
			rowMaps.add(row.map());
		}
		return rowMaps;
	}

	@SuppressWarnings("unchecked")
	private static List<Row> parseRows(Object[]  rowMaps) {
		List<Row> rows = Collections.synchronizedList(new ArrayList<Row>());
		for(Object rowMap : rowMaps) {
			rows.add(new Row((Map<String, Object>) rowMap));
		}
		return rows;
	}

	@Override
	public Map<String, Object> marshal() {
		HashMap<String, Object> m = new HashMap<String, Object> ();
		m.put("key", EventKey.marshal(key));
		m.put("created", created);
		m.put("expireTime", expireTime);
		m.put("timeoutTime", timeoutTime);
		m.put("maxRows", maxRows);
		m.put("host", host);
		m.put("rows", format(rows));
		m.put("variables", variables);
		m.put("count", counter.get());
		return m;
	}

	@SuppressWarnings("unchecked")
	public static EventContext parse(Map<String, Object> m) {
		EventKey key = EventKey.parse((String) m.get("key"));
		Long created = (Long) m.get("created");
		Long expireTime = (Long) m.get("expireTime");
		Long timeoutTime = (Long) m.get("timeoutTime");
		Integer maxRows = (Integer) m.get("maxRows");
		String host = (String) m.get("host");
		List<Row> rows = parseRows((Object[]) m.get("rows"));
		HashMap<String, Object> variables = (HashMap<String, Object>) m.get("variables"); 
		Integer count = (Integer) m.get("count");
		EventContext cxt = new EventContext(key,created, expireTime,  timeoutTime, maxRows, host);
		for(Row row : rows)
			cxt.addRow(row);

		if(count != null)
			cxt.counter.addAndGet(count);

		if(variables != null)
			cxt.variables = variables;

		return cxt;
	}

	public static EventContext merge(EventContext oldCtx, EventContext ctx) {
		if(ctx.getTimeoutTime()!= 0L)
			oldCtx.setTimeoutTime(ctx.getTimeoutTime());

		if(ctx.getHost() != null)
			oldCtx.setHost(ctx.getHost());

		for(Row row :  ctx.getRows()) {
			oldCtx.addRow(row);
		}

		for(String vKey : ctx.getVariables().keySet())
			oldCtx.setVariable(vKey, ctx.getVariable(vKey));

		oldCtx.getCounter().addAndGet(ctx.getCounter().get());

		return oldCtx;
	}
}


