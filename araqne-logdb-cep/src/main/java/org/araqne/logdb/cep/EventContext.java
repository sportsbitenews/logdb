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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;

import org.araqne.logdb.Row;

public class EventContext {
	private EventKey key;
	private List<Row> rows;

	private AtomicInteger counter = new AtomicInteger();

	// log time or real time
	private long created;

	// 0 means infinite, absolutely disappears
	private long expireTime;

	// 0 means infinite, extended when new row arrives
	private long timeoutTime;

	// 0 means infinite
	private int threshold;

	private int maxRows;

	// host for external log tick
	private String host;

	private CopyOnWriteArraySet<EventContextListener> listeners = new CopyOnWriteArraySet<EventContextListener>();

	public EventContext(EventKey key, long created, long expireTime, long timeoutTime, int threshold, int maxRows, String host) {
		this.key = key;
		this.created = created;
		this.rows = Collections.synchronizedList(new ArrayList<Row>());
		this.expireTime = expireTime;
		this.timeoutTime = timeoutTime;
		this.threshold = threshold;
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
		return Collections.unmodifiableList(rows);
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

	public int getThreshold() {
		return threshold;
	}

	public void setThreshold(int threshold) {
		this.threshold = threshold;
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
			if (rows.size() < maxRows)
				rows.add(row);
		}
	}

	public void removeRow(Row row) {
		synchronized (rows) {
			rows.remove(row);
		}
	}

	public CopyOnWriteArraySet<EventContextListener> getListeners() {
		return listeners;
	}

	public void setListeners(CopyOnWriteArraySet<EventContextListener> listeners) {
		this.listeners = listeners;
	}
}
