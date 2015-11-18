package org.araqne.logdb.query.parser;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.LookupHandlerRegistry;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Strings;
import org.araqne.logdb.query.command.Lookup;

public class LookupParser extends AbstractQueryCommandParser {
	private LookupHandlerRegistry registry;

	public LookupParser(LookupHandlerRegistry registry) {
		this.registry = registry;
		setDescriptions("Lookup field by key.", "매핑 테이블을 조회하여 특정한 필드 값을 다른 값으로 변환합니다.");
	}

	@Override
	public String getCommandName() {
		return "lookup";
	}

	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("20700", new QueryErrorMessage("output-token-not-found", "lookup 명령에서 output 구문이 누락되었습니다."));
		m.put("20701", new QueryErrorMessage("invalid-lookup-name", "lookup 명령에서 [table]은 유효하지 않는 매핑 테이블입니다."));
		m.put("20702", new QueryErrorMessage("invalid-lookup-field", "lookup 필드 문법이 유효하지 않습니다. `필드 as 별칭` 문법을 사용하십시오."));
		m.put("20703", new QueryErrorMessage("as-token-not-found", "lookup 명령의 [as] 자리에 as가 와야 합니다."));
		return m;
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		int last = getCommandName().length();
		int p = QueryTokenizer.skipSpaces(commandString, last);
		p = commandString.indexOf(' ', p);
		if (p < 0)
			throw new QueryParseException("20701", -1, -1, params("table", "빈 값"));

		String handlerName = commandString.substring(last, p).trim();
		if (registry != null && registry.getLookupHandler(handlerName) == null)
			throw new QueryParseException("20701", last + 1, p - 1, params("table", handlerName));

		last = p;

		p = commandString.toLowerCase().indexOf(" output ", last);
		if (p < 0)
			throw new QueryParseException("20700", getCommandName().length() + 1, commandString.length() - 1, null);

		String inputToken = commandString.substring(last, p);
		List<String> outputTokens = Strings.tokenize(commandString.substring(p + " output ".length()), ",");

		LookupField src = parseLookupField(inputToken, last, p);
		Map<String, String> outputFields = new HashMap<String, String>();
		for (String outputToken : outputTokens) {
			LookupField f = parseLookupField(outputToken, p + " output ".length(), commandString.length() - 1);
			outputFields.put(f.first, f.second);
		}

		return new Lookup(registry, handlerName, src.first, src.second, outputFields);
	}

	private LookupField parseLookupField(String s, int begin, int end) {
		LookupField field = new LookupField();
		List<String> tokens = Strings.tokenize(s, " ");

		if (tokens.size() == 1) {
			field.first = tokens.get(0);
			field.second = field.first;
			return field;
		}

		if (tokens.size() != 3)
			throw new QueryParseException("20702", begin, end, null);

		if (!tokens.get(1).equalsIgnoreCase("as"))
			throw new QueryParseException("20703", begin, end, params("as", tokens.get(1)));

		field.first = tokens.get(0);
		field.second = tokens.get(2);
		return field;
	}

	private class LookupField {
		private String first;
		private String second;
	}
}
