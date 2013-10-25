package org.araqne.logstorage.backup;

import java.util.Locale;
import java.util.Map;

public class StorageBackupConfigSpec {
	private String key;
	private Map<Locale, String> displayNames;
	private Map<Locale, String> descriptions;

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public Map<Locale, String> getDisplayNames() {
		return displayNames;
	}

	public void setDisplayNames(Map<Locale, String> displayNames) {
		this.displayNames = displayNames;
	}

	public Map<Locale, String> getDescriptions() {
		return descriptions;
	}

	public void setDescriptions(Map<Locale, String> descriptions) {
		this.descriptions = descriptions;
	}
}
