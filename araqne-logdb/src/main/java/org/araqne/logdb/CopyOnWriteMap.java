package org.araqne.logdb;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class CopyOnWriteMap implements Map<String, Object> {

	private boolean shared;
	private Map<String, Object> m;

	public CopyOnWriteMap(Map<String, Object> original) {
		this.shared = true;
		this.m = original;
	}

	@Override
	public int size() {
		return m.size();
	}

	@Override
	public boolean isEmpty() {
		return m.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return m.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return m.containsValue(value);
	}

	@Override
	public Object get(Object key) {
		return m.get(key);
	}

	@Override
	public Object put(String key, Object value) {
		if (shared) {
			m = Row.clone(m);
			shared = false;
		}

		return m.put(key, value);
	}

	@Override
	public Object remove(Object key) {
		if (shared) {
			m = Row.clone(m);
			shared = false;
		}

		return m.remove(key);
	}

	@Override
	public void putAll(Map<? extends String, ? extends Object> m) {
		if (shared) {
			this.m = Row.clone(this.m);
			shared = false;
		}

		this.m.putAll(m);
	}

	@Override
	public void clear() {
		if (shared) {
			m = new HashMap<String, Object>();
			shared = false;
		} else {
			m.clear();
		}
	}

	@Override
	public Set<String> keySet() {
		return m.keySet();
	}

	@Override
	public Collection<Object> values() {
		return m.values();
	}

	@Override
	public Set<java.util.Map.Entry<String, Object>> entrySet() {
		return m.entrySet();
	}
}
