/*
 * Copyright 2013 Eediom Inc.
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
package org.araqne.logdb;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Row {
	// _time cache
	private Date d;
	private final Map<String, Object> map;

	public Row() {
		this(new HashMap<String, Object>());
	}

	public Row(Map<String, Object> map) {
		this.map = map;
	}

	public Date getDate() {
		return d;
	}

	public Object get(String key) {
		return map.get(key);
	}

	public void put(String key, Object value) {
		if (key.equals("_time") && value instanceof Date)
			d = (Date) value;
		map.put(key, value);
	}

	public Object remove(String key) {
		return map.remove(key);
	}

	public boolean containsKey(String key) {
		return map.containsKey(key);
	}

	public Map<String, Object> map() {
		return map;
	}

	@Override
	public String toString() {
		if (map != null)
			return map.toString();
		return "null";
	}

}