package org.araqne.logdb.query.command;

import java.util.List;
import java.util.Map;

import org.araqne.log.api.CsvParser;
import org.araqne.logdb.FieldOrdering;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.ThreadSafe;
import org.slf4j.LoggerFactory;

public class ParseCsv extends QueryCommand implements FieldOrdering, ThreadSafe {
	private final org.slf4j.Logger slog = LoggerFactory.getLogger(ParseCsv.class);
	private String field;
	private boolean overlay;
	private char delimiter;
	private char escape;
	private List<String> fieldNames;
	private CsvParser parser;

	public ParseCsv(String field, boolean overlay, boolean useTab, List<String> fieldNames) {
		this.field = field;
		this.overlay = overlay;
		this.fieldNames = fieldNames;
		parser = new CsvParser(useTab, false, fieldNames == null ? null : fieldNames.toArray(new String[0]));
	}

	@Override
	public String getName() {
		return "parsecsv";
	}

	@Override
	public List<String> getFieldOrder() {
		return fieldNames;
	}

	@Override
	public void onPush(Row row) {
		pushPipe(parse(row));
	}

	@Override
	public void onPush(RowBatch rowBatch) {
		if (rowBatch.selectedInUse) {
			for (int i = 0; i < rowBatch.size; i++) {
				int p = rowBatch.selected[i];
				Row row = rowBatch.rows[p];
				rowBatch.rows[p] = parse(row);
			}
		} else {
			for (int i = 0; i < rowBatch.size; i++) {
				Row row = rowBatch.rows[i];
				rowBatch.rows[i] = parse(row);
			}
		}

		pushPipe(rowBatch);
	}

	private Row parse(Row row) {
		String target = (String) row.get(field);
		if (target == null)
			return row;

		try {
			Map<String, Object> m = parser.parse(target);
			if (overlay)
				row.map().putAll(m);
			else
				row = new Row(m);
		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne logdb: cannot parse csv [{}], error [{}]", target, t.getMessage());
		}

		return row;
	}

	@Override
	public String toString() {
		String fields = "";
		if (fieldNames != null) {
			for (int i = 0; i < fieldNames.size(); i++) {
				fields += fieldNames.get(i);
				if (i != fieldNames.size() - 1)
					fields += ",";
			}
		}

		return "parsecsv field=" + field + " overlay=" + overlay + " delimiter=" + delimiter + " escape=" + escape + " " + fields;
	}
}
