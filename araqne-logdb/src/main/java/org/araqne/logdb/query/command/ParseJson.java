/*
 * Copyright 2014 Eediom Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.araqne.logdb.query.command;

import java.io.StringReader;
import java.util.Map;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.ThreadSafe;
import org.json.JSONArray;
import org.json.JSONConverter;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.LoggerFactory;

public class ParseJson extends QueryCommand implements ThreadSafe {
	private final org.slf4j.Logger slog = LoggerFactory.getLogger(ParseJson.class);
	private final String field;
	private final boolean overlay;

	public ParseJson(String field, boolean overlay) {
		this.field = field;
		this.overlay = overlay;
	}

	@Override
	public String getName() {
		return "parsejson";
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
		if (target == null)
			return row;

		String text = target.toString();
		JSONTokener tokenizer = new JSONTokener(new StringReader(text));

		try {
			Object value = tokenizer.nextValue();

			if (value instanceof JSONObject) {
				Map<String, Object> m = JSONConverter.parse((JSONObject) value);
				if (overlay)
					row.map().putAll(m);
				else
					row = new Row(m);

			} else if (value instanceof JSONArray) {
				Object array = JSONConverter.parse((JSONArray) value);
				row.put(field, array);
			}
		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne logdb: cannot parse json [{}]", text);
		}
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

		return "parsejson" + fieldOpt + overlayOpt;
	}
}
