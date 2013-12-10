package org.araqne.logdb.query.parser;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryCommandParser;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.query.command.Json;
import org.json.JSONArray;
import org.json.JSONConverter;
import org.json.JSONObject;
import org.json.JSONTokener;

public class JsonParser implements QueryCommandParser {

	@Override
	public String getCommandName() {
		return "json";
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		String text = commandString.substring(getCommandName().length()).trim();
		if (!text.startsWith("\"") || !text.endsWith("\""))
			throw new QueryParseException("missing-json-quotation", -1);

		String json = text;
		text = text.substring(1, text.length() - 1);
		text = text.replaceAll("\\\\\\\\", "\\\\").replaceAll("\\\\\"", "\"");

		List<Row> logs = new ArrayList<Row>();
		try {
			JSONTokener tokenizer = new JSONTokener(new StringReader(text));
			Object value = tokenizer.nextValue();

			if (value instanceof JSONObject) {
				logs.add(new Row(JSONConverter.parse((JSONObject) value)));
			} else if (value instanceof JSONArray) {
				List<Object> l = (List<Object>) JSONConverter.parse((JSONArray) value);
				for (Object o : l)
					if (o instanceof Map)
						logs.add(new Row((Map<String, Object>) o));
			} else
				throw new QueryParseException("invalid-json-type", -1);
		} catch (Throwable t) {
			throw new QueryParseException("invalid-json", -1);
		}

		return new Json(logs, json);
	}
}
