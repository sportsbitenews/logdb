package org.araqne.logdb.query.parser;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.LogMap;
import org.araqne.logdb.LogQueryCommand;
import org.araqne.logdb.LogQueryCommandParser;
import org.araqne.logdb.LogQueryContext;
import org.araqne.logdb.LogQueryParseException;
import org.araqne.logdb.query.command.Json;
import org.json.JSONArray;
import org.json.JSONConverter;
import org.json.JSONObject;
import org.json.JSONTokener;

public class JsonParser implements LogQueryCommandParser {

	@Override
	public String getCommandName() {
		return "json";
	}

	@SuppressWarnings("unchecked")
	@Override
	public LogQueryCommand parse(LogQueryContext context, String commandString) {
		String text = commandString.substring(getCommandName().length()).trim();
		if (!text.startsWith("\"") || !text.endsWith("\""))
			throw new LogQueryParseException("missing-json-quotation", -1);

		text = text.substring(1, text.length() - 1);
		text = text.replaceAll("\\\\\\\\", "\\\\").replaceAll("\\\\\"", "\"");

		List<LogMap> logs = new ArrayList<LogMap>();
		try {
			JSONTokener tokenizer = new JSONTokener(new StringReader(text));
			Object value = tokenizer.nextValue();

			if (value instanceof JSONObject) {
				logs.add(new LogMap(JSONConverter.parse((JSONObject) value)));
			} else if (value instanceof JSONArray) {
				List<Object> l = (List<Object>) JSONConverter.parse((JSONArray) value);
				for (Object o : l)
					if (o instanceof Map)
						logs.add(new LogMap((Map<String, Object>) o));
			} else
				throw new LogQueryParseException("invalid-json-type", -1);
		} catch (Throwable t) {
			throw new LogQueryParseException("invalid-json", -1);
		}

		return new Json(logs);
	}
}
