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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class Row {
	private final Map<String, Object> map;

	public Row() {
		this(new HashMap<String, Object>());
	}

	public Row(Map<String, Object> map) {
		this.map = map;
	}

	public Row clone() {
		return new Row(new CopyOnWriteMap(map));
	}

	@SuppressWarnings("unchecked")
	public static Map<String, Object> clone(Map<String, Object> m) {
		HashMap<String, Object> cloned = new HashMap<String, Object>();
		for (Entry<String, Object> e : m.entrySet()) {
			String key = e.getKey();
			Object val = e.getValue();

			if (val instanceof Map)
				cloned.put(key, clone((Map<String, Object>) val));
			else if (val instanceof Collection)
				cloned.put(key, clone((Collection<Object>) val));
			else
				cloned.put(key, val);
		}
		return cloned;
	}

	@SuppressWarnings("unchecked")
	public static List<Object> clone(Collection<Object> c) {
		ArrayList<Object> l = new ArrayList<Object>(c.size());
		for (Object o : c) {
			if (o instanceof Map)
				l.add(clone((Map<String, Object>) o));
			else if (o instanceof Collection)
				l.add(clone((Collection<Object>) o));
			else
				l.add(o);
		}
		return l;
	}

	public Date getDate() {
		Object o = map.get("_time");
		if (o instanceof Date)
			return (Date) o;
		return null;
	}

	public Object get(String key) {
		return map.get(key);
	}

	/**
	 * @since 2.4.19
	 */
	public String getString(String key) {
		Object o = map.get(key);
		if (o == null)
			return null;
		return o.toString();
	}

	public void put(String key, Object value) {
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