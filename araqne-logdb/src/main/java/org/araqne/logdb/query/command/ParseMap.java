package org.araqne.logdb.query.command;

import java.util.Map;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.ThreadSafe;

public class ParseMap extends QueryCommand implements ThreadSafe {
	private final String field;
	private final boolean overlay;

	public ParseMap(String field, boolean overlay) {
		this.field = field;
		this.overlay = overlay;
	}

	@Override
	public String getName() {
		return "parsemap";
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
		Object target = row.get(field);
		if (target == null || !(target instanceof Map))
			return row;

		@SuppressWarnings("unchecked")
		Map<String, Object> m = (Map<String, Object>) target;
		if (overlay)
			row.map().putAll(m);
		else
			row = new Row(m);

		return row;
	}

	@Override
	public String toString() {
		String fieldOpt = "";
		if (field != null && !field.equals("line"))
			fieldOpt = " field=" + field;

		String overlayOpt = "";
		if (overlay)
			overlayOpt = " overlay=t";

		return "parsemap" + fieldOpt + overlayOpt;
	}
}
