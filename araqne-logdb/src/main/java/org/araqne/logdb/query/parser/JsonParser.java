package org.araqne.logdb.query.parser;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;
import org.araqne.logdb.query.command.Json;
import org.json.JSONArray;
import org.json.JSONConverter;
import org.json.JSONObject;
import org.json.JSONTokener;

public class JsonParser extends AbstractQueryCommandParser {

	@Override
	public String getCommandName() {
		return "json";
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		String text = commandString.substring(getCommandName().length()).trim();

		if (!text.startsWith("\"") || !text.endsWith("\""))
		//throw new QueryParseException("missing-json-quotation", -1);
			throw new QueryParseException("10200", getCommandName().length() + 1, commandString.length() -1 , null);

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
			} else{
				//throw new QueryParseException("invalid-json-type", -1);
				throw new QueryParseException("10201", commandString.indexOf(json.charAt(0)), commandString.length() -1, null);
			}
		}catch(QueryParseException e){
			throw e;
		}catch (Throwable t) {
			//throw new QueryParseException("invalid-json", -1);
			Map<String, String> param = new HashMap<String, String>();
			param.put("msg", t.getMessage());
			throw new QueryParseException("10202", commandString.indexOf(json.charAt(0)) , commandString.length() -1, param);
		}
		return new Json(logs, json);
	}
}
