package org.araqne.logdb;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.araqne.api.FieldOption;
import org.araqne.msgbus.Marshalable;

public class ProcedureParameter implements Marshalable {
	private static final Set<String> ACCEPTED_TYPES = new HashSet<String>(Arrays.asList("string", "int", "double", "bool",
			"datetime", "date"));

	@FieldOption(nullable = false)
	private String key;

	/**
	 * string, int, double, bool, datetime, date (do not support short, long,
	 * float variables)
	 */
	@FieldOption(nullable = false)
	private String type;

	// key will be printed if name is null
	@FieldOption(nullable = true)
	private String name;

	@FieldOption(nullable = true)
	private String description;

	public ProcedureParameter() {
	}

	public ProcedureParameter(String key, String type) {
		this.key = key;
		this.type = type;
	}

	public ProcedureParameter clone() {
		ProcedureParameter pp = new ProcedureParameter();
		pp.setKey(key);
		pp.setType(type);
		pp.setName(name);
		pp.setDescription(description);
		return pp;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		if (type == null)
			throw new IllegalArgumentException("null procedure parameter type");

		if (!ACCEPTED_TYPES.contains(type))
			throw new IllegalArgumentException("invalid procedure parameter type: " + type);

		this.type = type;
	}

	@Override
	public Map<String, Object> marshal() {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("key", key);
		m.put("type", type);
		m.put("name", name);
		m.put("description", description);
		return m;
	}

	@Override
	public String toString() {
		return type + " " + key;
	}

}
