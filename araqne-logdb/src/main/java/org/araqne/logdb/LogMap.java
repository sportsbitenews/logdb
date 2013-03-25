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