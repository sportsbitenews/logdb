package org.araqne.logdb.query.command;

import java.util.Map;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.Row;

public class ParseMap extends QueryCommand {
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
		Object target = row.get(field);
		if (target == null || !(target instanceof Map)) {
			if (overlay)
				pushPipe(row);
			return;
		}

		@SuppressWarnings("unchecked")
		Map<String, Object> m = (Map<String, Object>) target;
		if (overlay)
			row.map().putAll(m);
		else
			row = new Row(m);

		pushPipe(row);
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
