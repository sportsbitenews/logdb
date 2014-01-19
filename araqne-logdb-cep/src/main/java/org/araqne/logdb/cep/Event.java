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
import java.util.Date;
import java.util.List;

import org.araqne.logdb.Row;

public class Event {
	private EventKey key;
	private EventCause cause;
	private List<Row> rows = new ArrayList<Row>();
	private Date created = new Date();

	public Event(EventKey key, EventCause cause) {
		if (key == null)
			throw new IllegalArgumentException("event key cannot be null");

		this.key = key;
		this.cause = cause;
	}

	public EventKey getKey() {
		return key;
	}

	public EventCause getCause() {
		return cause;
	}

	public List<Row> getRows() {
		return rows;
	}

	public Date getCreated() {
		return created;
	}
}
