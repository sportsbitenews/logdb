package org.araqne.logdb;

import org.araqne.api.FieldOption;

public class ProcedureVariable {
	@FieldOption(nullable = false)
	private String key;

	/**
	 * string, int, double, bool (do not support date, short, long, float
	 * variables)
	 */
	@FieldOption(nullable = false)
	private String type;

	// key will be printed if name is null
	@FieldOption(nullable = true)
	private String name;

	@FieldOption(nullable = true)
	private String description;

	public ProcedureVariable() {
	}

	public ProcedureVariable(String key, String type) {
		this.key = key;
		this.type = type;
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
		this.type = type;
	}

	@Override
	public String toString() {
		return "var " + type + " " + key;
	}

}
