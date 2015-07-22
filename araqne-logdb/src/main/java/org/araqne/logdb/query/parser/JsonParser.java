package org.araqne.logdb.query.parser;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
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

	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("10200", new QueryErrorMessage("missing-json-quotation", "json 문자열은 큰 따옴표(\")로 시작하고 끝나야 합니다."));
		m.put("10201", new QueryErrorMessage("invalid-json-type", "json 형태의 문자열을 입력하십시오."));
		m.put("10202", new QueryErrorMessage("invalid-json", "json 파싱에 실패하였습니다. [msg]"));
		return m;
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		String literal = commandString.substring(getCommandName().length()).trim();
		boolean isConstant = !ExpressionParser.isContextReference(literal);
		String json = ExpressionParser.evalContextReference(context, literal, getFunctionRegistry());

		if (isConstant) {
			if (!json.startsWith("\"") || !json.endsWith("\""))
				// throw new QueryParseException("missing-json-quotation", -1);
				throw new QueryParseException("10200", getCommandName().length() + 1, commandString.length() - 1, null);

			json = json.substring(1, json.length() - 1);
			json = json.replace("\\\"", "\"").replace("\\\\", "\\");
		}

		List<Row> logs = new ArrayList<Row>();
		try {
			JSONTokener tokenizer = new JSONTokener(new StringReader(json));
			Object value = tokenizer.nextValue();

			if (value instanceof JSONObject) {
				logs.add(new Row(JSONConverter.parse((JSONObject) value)));
			} else if (value instanceof JSONArray) {
				List<Object> l = (List<Object>) JSONConverter.parse((JSONArray) value);
				for (Object o : l)
					if (o instanceof Map)
						logs.add(new Row((Map<String, Object>) o));
			} else {
				// throw new QueryParseException("invalid-json-type", -1);
				throw new QueryParseException("10201", commandString.indexOf(json.charAt(0)), commandString.length() - 1, null);
			}
		} catch (QueryParseException e) {
			throw e;
		} catch (Throwable t) {
			// throw new QueryParseException("invalid-json", -1);
			Map<String, String> param = new HashMap<String, String>();
			param.put("msg", t.getMessage());
			throw new QueryParseException("10202", commandString.indexOf(json.charAt(0)), commandString.length() - 1, param);
		}
		return new Json(logs, literal);
	}
}
