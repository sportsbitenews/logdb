package org.araqne.logstorage.dump;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class DumpConfigSpec {
	private String key;
	private Map<Locale, String> displayNames;
	private Map<Locale, String> descriptions;
	private boolean required;

	public static Map<Locale, String> t(String en, String ko) {
		Map<Locale, String> m = new HashMap<Locale, String>();
		m.put(Locale.ENGLISH, en);
		m.put(Locale.KOREAN, ko);
		return m;
	}

	public DumpConfigSpec() {
	}

	public DumpConfigSpec(String key, Map<Locale, String> displayNames, Map<Locale, String> descriptions, boolean required) {
		this.key = key;
		this.displayNames = displayNames;
		this.descriptions = descriptions;
		this.required = required;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getDisplayName(Locale locale) {
		String text = displayNames.get(locale);
		if (text != null)
			return text;

		return displayNames.get(Locale.ENGLISH);
	}

	public Map<Locale, String> getDisplayNames() {
		return displayNames;
	}

	public void setDisplayNames(Map<Locale, String> displayNames) {
		this.displayNames = displayNames;
	}

	public String getDescription(Locale locale) {
		String text = descriptions.get(locale);
		if (text != null)
			return text;

		return descriptions.get(Locale.ENGLISH);
	}

	public Map<Locale, String> getDescriptions() {
		return descriptions;
	}

	public void setDescriptions(Map<Locale, String> descriptions) {
		this.descriptions = descriptions;
	}

	public boolean isRequired() {
		return required;
	}

	public void setRequired(boolean required) {
		this.required = required;
	}
}
