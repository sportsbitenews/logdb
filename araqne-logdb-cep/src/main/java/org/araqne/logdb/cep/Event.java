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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.Row;

public class Event {
	private EventKey key;
	private EventCause cause;
	private int counter;
	private Map<String, Object> variables;
	private List<Row> rows = new ArrayList<Row>();
	private Date created;

	public Event(EventContext ctx, EventCause cause) {
		this.key = ctx.getKey();
		if (key == null)
			throw new IllegalArgumentException("event key cannot be null");

		this.cause = cause;
		this.created = new Date(ctx.getCreated());
		this.counter = ctx.getCounter().get();
		this.variables = new HashMap<String, Object>(ctx.getVariables());
	}

	public EventKey getKey() {
		return key;
	}

	public EventCause getCause() {
		return cause;
	}

	public int getCounter() {
		return counter;
	}

	public Map<String, Object> getVariables() {
		return variables;
	}

	public List<Row> getRows() {
		return rows;
	}

	public Date getCreated() {
		return created;
	}

	@Override
	public String toString() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return "event key=" + key + ", cause=" + cause + ", rows=" + rows + ", created=" + df.format(created);
	}
}
