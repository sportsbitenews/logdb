package org.araqne.log.api;

import java.util.HashMap;
import java.util.Map;

public class FixedLengthLogParser extends V1LogParser {
	private String targetField;
	private boolean includeTargetField;
	private Integer[] fieldLength;
	private String[] columnHeaders;

	public FixedLengthLogParser(String targetField, boolean includeTargetField, Integer[] fieldLength, String[] columnHeaders) {
		this.fieldLength = fieldLength;
		this.columnHeaders = columnHeaders;
		this.targetField = targetField;
		this.includeTargetField = includeTargetField;
	}

	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		String line = (String) params.get(targetField);
		if (line == null)
			return params;

		int startIndex = 0;
		int endIndex = 0;
		Map<String, Object> m = new HashMap<String, Object>();
		for (int i = 0; i < columnHeaders.length; i++) {
			if (i >= fieldLength.length) {
				m.put(columnHeaders[i], null);
				continue;
			}

			startIndex = endIndex;
			endIndex += fieldLength[i];

			String value = null;
			if (startIndex < line.length()) {
				if (endIndex > line.length())
					value = line.substring(startIndex).trim();
				else
					value = line.substring(startIndex, endIndex).trim();
			} else
				value = null;

			m.put(columnHeaders[i], value);
		}

		if (this.includeTargetField)
			m.put(targetField, line);

		return m;
	}

}
