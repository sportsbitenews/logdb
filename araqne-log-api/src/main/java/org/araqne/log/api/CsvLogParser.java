package org.araqne.log.api;

import java.util.Map;

public class CsvLogParser extends V1LogParser {
	private final String targetField;
	private final boolean includeTargetField;
	private final String[] columnHeaders;
	private CsvParser parser;

	public CsvLogParser(boolean useTab, boolean useDoubleQuote, String[] columnHeaders, String targetField,
			boolean includeTargetField) {
		this.targetField = targetField;
		this.columnHeaders = columnHeaders;
		this.includeTargetField = includeTargetField;
		parser = new CsvParser(useTab, useDoubleQuote, columnHeaders);
	}

	public String[] getColumnHeaders() {
		return columnHeaders;
	}

	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		String line = (String) params.get(targetField);
		if (line == null)
			return params;

		Map<String, Object> m = parser.parse(line);
		if (includeTargetField)
			m.put(targetField, line);

		return m;
	}
}
