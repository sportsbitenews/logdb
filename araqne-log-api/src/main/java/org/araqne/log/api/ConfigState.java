package org.araqne.log.api;

import java.util.HashMap;
import java.util.Map;

public class ConfigState {
	private String hash;

	public ConfigState() {
	}

	public ConfigState(String hash) {
		this.hash = hash;
	}

	public static Map<String, Object> serializeMap(Map<String, ConfigState> states) {
		Map<String, Object> m = new HashMap<String, Object>();
		for (String key : states.keySet()) {
			ConfigState state = states.get(key);
			m.put(key, state == null ? null : state.serialize());
		}
		return m;
	}

	@SuppressWarnings("unchecked")
	public static Map<String, ConfigState> deserializeMap(Map<String, Object> states) {
		Map<String, ConfigState> m = new HashMap<String, ConfigState>();
		for (String path : states.keySet()) {
			ConfigState state = deserialize((Map<String, Object>) states.get(path));
			m.put(path, state);
		}

		return m;
	}

	public static ConfigState deserialize(Map<String, Object> m) {
		ConfigState s = new ConfigState();
		s.hash = (String) m.get("hash");
		return s;
	}

	public Map<String, Object> serialize() {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("hash", hash);
		return m;
	}

	public String getHash() {
		return hash;
	}

	public void setHash(String hash) {
		this.hash = hash;
	}
}