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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.araqne.logdb.Row;

public class EventContext implements EventClockItem {
	private EventKey key;
	private List<Row> rows;

	private AtomicInteger counter = new AtomicInteger();

	// log time or real time
	private long created;

	// 0 means infinite, absolutely disappears
	private long expireTime;

	// 0 means infinite, extended when new row arrives
	private AtomicLong timeoutTime;

	private int maxRows;

	private HashMap<String, Object> variables = new HashMap<String, Object>(1);

	private CopyOnWriteArraySet<EventContextListener> listeners = new CopyOnWriteArraySet<EventContextListener>();

	public EventContext(EventKey key, long created, long expireTime, long timeoutTime, int maxRows) {
		this.key = key;
		this.created = created;
		this.rows = Collections.synchronizedList(new ArrayList<Row>());
		this.expireTime = expireTime;
		this.timeoutTime = new AtomicLong(timeoutTime);
		this.maxRows = maxRows;
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
		return timeoutTime.get();
	}

	public void setTimeoutTime(long newTimeoutTime) {
		// set only if when timeout time is increased
		while (true) {
			long l = timeoutTime.get();
			if (newTimeoutTime < l)
				break;

			if (timeoutTime.compareAndSet(l, newTimeoutTime)) {
				for (EventContextListener listener : listeners) {
					listener.onUpdateTimeout(this);
				}
				break;
			}
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

	@Override
	public String getHost() {
		return key.getHost();
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

	public EventContext clone() {
		EventContext context = new EventContext(key, created, expireTime, timeoutTime.get(), maxRows);
		context.counter.set(counter.get());

		for (Row row : rows)
			context.addRow(row);

		for (Entry<String, Object> entry : getVariables().entrySet())
			context.setVariable(entry.getKey(), entry.getValue());

		return context;
	}

	@Override
	public int hashCode() {
		return key.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;

		// must be same class
		if (getClass() != obj.getClass())
			return false;

		// topic and key is always not null
		EventContext other = (EventContext) obj;

		return key.equals(other.getKey()) && created == other.getCreated() && expireTime == other.getExpireTime()
				&& timeoutTime.get() == other.getTimeoutTime() && maxRows == other.getMaxRows()
				&& counter.get() == other.getCounter().get()
				&& equalRowLists(other.getRows())
				&& variables.equals(other.getVariables());
	}

	public boolean equalRowLists(List<Row> one) {
		if (rows == null && one == null) {
			return true;
		}

		if ((one == null && rows != null) || one != null && rows == null || one.size() != rows.size()) {
			return false;
		}

		for(int i = 0; i < rows.size(); i++) {
			Map<String, Object> map1 = rows.get(i).map();
			Map<String, Object> map2 = one.get(i).map();

			if (map1 == null && map2 == null) {
				continue;
			}

			if ((map1 == null && map2 != null) || map1 != null && map2 == null || map1.size() != map2.size() || !map1.equals(map2)) {
				return false;
			}
		}

		return true;
	}		

	@Override
	public String toString() {
		return "[key = " + key.toString() + ", create = " + new Date(created) + ", expire time = "
				+ new Date(expireTime) + ", timeout time = " + new Date(timeoutTime.get()) + ", max rows = " + maxRows
				+ ", counter = " + counter.get() + ", rows = " + rows + ", variables = " + variables + "]";
	}

}
