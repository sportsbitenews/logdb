package org.araqne.logdb;

import java.util.Collections;
import java.util.Map;

public class MemLookupHandler implements LookupHandler2 {
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

	@Override
	public LookupTable newTable(String keyField, Map<String, String> outputFields) {
		return new MemLookupTable(keyField, outputFields);
	}

	private class MemLookupTable implements LookupTable {

		private String keyField;
		private Map<String, String> outputFields;

		public MemLookupTable(String keyField, Map<String, String> outputFields) {
			this.keyField = keyField;
			this.outputFields = outputFields;
		}

		@Override
		public void lookup(Row row) {
			lookupRow(row);
		}

		@Override
		public void lookup(RowBatch rowBatch) {
			if (rowBatch.selectedInUse) {
				for (int i = 0; i < rowBatch.size; i++) {
					int p = rowBatch.selected[i];
					Row row = rowBatch.rows[p];
					lookupRow(row);
				}
			} else {
				for (int i = 0; i < rowBatch.size; i++) {
					Row row = rowBatch.rows[i];
					lookupRow(row);
				}
			}
		}

		private void lookupRow(Row row) {
			Object key = row.get(keyField);
			if (key == null)
				return;

			Map<String, Object> m = mappings.get(key);
			if (m == null)
				return;

			for (String name : outputFields.keySet()) {
				String renamed = outputFields.get(name);
				row.put(renamed, m.get(name));
			}
		}
	}
}
