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

import java.util.HashMap;
import java.util.Map;

public class LogMap {
	private Map<String, Object> map;

	public LogMap() {
		this(new HashMap<String, Object>());
	}

	public LogMap(Map<String, Object> map) {
		this.map = map;
	}

	public Object get(String key) {
		return get(map, key);
	}

	@SuppressWarnings("unchecked")
	private Object get(Map<String, Object> m, String key) {
		if (key == null)
			return null;

		if (!key.endsWith("]") || !key.contains("["))
			return m.get(key);

		int begin = key.indexOf("[");
		String thisKey = key.substring(0, begin);
		if (map.containsKey(thisKey) && (map.get(thisKey) instanceof Map)) {
			int end = key.lastIndexOf("]");
			return get((Map<String, Object>) map.get(thisKey), key.substring(begin + 1, end));
		} else
			return m.get(key);
	}

	public void put(String key, Object value) {
		map.put(key, value);
	}

	public void remove(String key) {
		map.remove(key);
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