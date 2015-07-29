package org.araqne.logdb;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class QueryCommandOption {
	private String key;
	private boolean required;
	private Map<Locale, String> descriptions = new HashMap<Locale, String>();
	
	public QueryCommandOption(String key) {
		this(key, false);
	}

	public QueryCommandOption(String key, boolean optional) {
		this.key = key;
		this.required = !optional;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public boolean isRequired() {
		return required;
	}

	public void setRequired(boolean required) {
		this.required = required;
	}

	public void setDescription(Locale locale, String text) {
		descriptions.put(locale, text);
	}

	public Map<Locale, String> getDescriptions() {
		return descriptions;
	}

	public void setDescriptions(Map<Locale, String> descriptions) {
		this.descriptions = descriptions;
	}
}
