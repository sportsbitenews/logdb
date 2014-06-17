package org.araqne.logdb.query.command;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.Row;

import au.com.bytecode.opencsv.CSVReader;

public class ParseCsv extends QueryCommand {
	private String field;
	private List<String> fieldNames;

	public ParseCsv(String field, List<String> fieldNames) {
		this.field = field;
		this.fieldNames = fieldNames;
	}

	@Override
	public String getName() {
		return "parsecsv";
	}

	@Override
	public void onPush(Row row) {
		String target = (String) row.get(field);
		if (target == null)
			return;

		CSVReader reader = null;
		try {
			reader = new CSVReader(new StringReader(target));
			int index = 0;
			String[] values = reader.readNext();
			if (fieldNames == null || fieldNames.isEmpty()) {
				for (String value : values) {
					row.map().put("column" + ++index, value);
				}
			} else {
				for (String fieldName : fieldNames) {
					row.map().put(fieldName, values[index++]);
				}
			}
			pushPipe(row);
		} catch (IOException e) {
		} finally {
			if (reader != null)
				try {
					reader.close();
				} catch (IOException e) {
				}
		}
	}
}
