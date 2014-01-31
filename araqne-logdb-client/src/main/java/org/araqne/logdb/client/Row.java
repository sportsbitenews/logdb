package org.araqne.logdb.client;

import java.util.HashMap;
import java.util.Map;

public class Row {
	private final Map<String, Object> map;

	public Row() {
		this(new HashMap<String, Object>());
	}

	public Row(Map<String, Object> map) {
		this.map = map;
	}

	public Object get(String key) {
		return map.get(key);
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
