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
import org.json.JSONConverter;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.LoggerFactory;

public class ParseJson extends QueryCommand {
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
		Object target = row.get(field);
		if (target == null) {
			if (overlay)
				pushPipe(row);
			return;
		}

		String text = target.toString();
		JSONTokener tokenizer = new JSONTokener(new StringReader(text));

		try {
			Object value = tokenizer.nextValue();
			Map<String, Object> m = JSONConverter.parse((JSONObject) value);
			if (overlay)
				row.map().putAll(m);
			else
				row = new Row(m);

			pushPipe(row);
		} catch (Throwable t) {
			if (overlay)
				pushPipe(row);

			if (slog.isDebugEnabled())
				slog.debug("araqne logdb: cannot parse json [{}]", text);
		}
	}

	@Override
	public String toString() {
		return "parsejson field=" + field + " overlay=" + overlay;
	}

}
