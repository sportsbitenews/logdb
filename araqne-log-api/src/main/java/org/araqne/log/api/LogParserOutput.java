package org.araqne.log.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LogParserOutput {
	private List<Map<String, Object>> rows;

	public LogParserOutput() {
		rows = new ArrayList<Map<String, Object>>(1);
	}

	public LogParserOutput(int capacity) {
		rows = new ArrayList<Map<String, Object>>(capacity);
	}

	public Map<String, Object> getFirst() {
		if (rows.size() > 0)
			return rows.get(0);
		return null;
	}

	public List<Map<String, Object>> getRows() {
		return rows;
	}
}
