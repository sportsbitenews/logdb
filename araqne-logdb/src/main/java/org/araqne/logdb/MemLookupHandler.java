package org.araqne.logdb;

import java.util.Collections;
import java.util.Map;

public class MemLookupHandler implements LookupHandler {
	private String keyField;
	private Map<String, Map<String, Object>> mappings;

	public MemLookupHandler(String keyField, Map<String, Map<String, Object>> mappings) {
		this.keyField = keyField;
		this.mappings = mappings;
	}

	public String getKeyField() {
		return keyField;
	}

	public Map<String, Map<String, Object>> getMappings() {
		return Collections.unmodifiableMap(mappings);
	}

	@Override
	public Object lookup(String srcField, String dstField, Object value) {
		Map<String, Object> row = mappings.get(value);
		if (row == null)
			return null;

		return row.get(dstField);
	}
}
