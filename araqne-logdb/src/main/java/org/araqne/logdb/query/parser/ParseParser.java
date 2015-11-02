/*
 * Copyright 2013 Eediom Inc.
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
package org.araqne.logdb.query.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.log.api.LogParserRegistry;
import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.Parse;
import org.araqne.logdb.query.command.ParseWithAnchor;

/**
 * @since 1.6.6
 * @author xeraph
 * 
 */
public class ParseParser extends AbstractQueryCommandParser {

	private LogParserRegistry registry;

	public ParseParser(LogParserRegistry registry) {
		this.registry = registry;
		setDescriptions("Parse input tuples.", "파서를 이용하여 입력 데이터를 파싱한 결과를 출력합니다.");
		setOptions("field", false, "Specify target field name. Default value is `line`.", "대상 필드를 별도로 지정하지 않는 경우 기본값은 line입니다.");
		setOptions("overlay", false, "Use `overlay=t` option if you want to override parsed fields on original data.",
				"t로 주면, 원본 필드에  추출된 필드를 덮어씌운 결과를 출력으로 내보냅니다. 별도로 overlay 옵션을 지정하지 않으면, 원본 데이터를 파싱한 결과만 출력으로 내보냅니다.");
	}

	@Override
	public String getCommandName() {
		return "parse";
	}

	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("21000", new QueryErrorMessage("missing-parser-name", "파서 이름을 입력하십시오."));
		m.put("21001", new QueryErrorMessage("parser-not-found", "[parser] 파서가 존재하지 않습니다."));
		m.put("21002", new QueryErrorMessage("parser-init-failure", "[parser] 파서 초기화에 실패했습니다."));
		m.put("21003", new QueryErrorMessage("syntax-error: \"as\" needed", "구문 안에\"as\" 가 필요합니다."));
		m.put("21004", new QueryErrorMessage("syntax-error: anchor should be quoted", "anchor 값([anc])은 쌍따옴표로 묶여야 합니다."));
		m.put("21005", new QueryErrorMessage("syntax-error: alias should not be quoted", "alias 값([alias])은 쌍따옴표로 묶여야 합니다."));
		return m;
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(),
				Arrays.asList("overlay", "field"), getFunctionRegistry());
		Map<String, String> options = (Map<String, String>) r.value;
		boolean overlay = CommandOptions.parseBoolean(options.get("overlay"));
		String field = options.get("field");
		String remainder = commandString.substring(r.next).trim();

		if (remainder.isEmpty())
			// throw new QueryParseException("missing-parameter", r.next);
			throw new QueryParseException("21000", getCommandName().length() + 1, commandString.length() - 1, null);

		if (QueryTokenizer.tokenize(remainder).size() == 1) {
			return newParserFromRegistry(overlay, remainder);
		}

		List<String> parseByComma = QueryTokenizer.parseByComma(remainder);
		List<String> anchors = new ArrayList<String>(parseByComma.size());
		List<String> aliases = new ArrayList<String>(parseByComma.size());

		for (String e : parseByComma) {
			e = e.trim();
			try {
				ParseResult anchor = QueryTokenizer.nextString(e);
				ParseResult as = QueryTokenizer.nextString(e, anchor.next);
				ParseResult alias = QueryTokenizer.nextString(e, as.next);

				if (!"as".equals(as.value))
					// throw new QueryParseException("syntax-error: \"as\"
					// needed", remainder.indexOf(e));
					throw new QueryParseException("21003", -1, -1, null);

				if (!QueryTokenizer.isQuoted((String) anchor.value)) {
					// throw new QueryParseException("syntax-error: anchor
					// should be quoted", remainder.indexOf(e));
					Map<String, String> params = new HashMap<String, String>();
					params.put("anc", (String) anchor.value);
					throw new QueryParseException("21004", -1, -1, null);
				}

				if (QueryTokenizer.isQuoted((String) alias.value)) {
					// throw new QueryParseException("syntax-error: alias should
					// not be quoted", remainder.indexOf(e));
					Map<String, String> params = new HashMap<String, String>();
					params.put("alias", (String) alias.value);
					throw new QueryParseException("21005", -1, -1, null);
				}

				anchors.add(unquote((String) anchor.value));
				aliases.add((String) alias.value);
			} catch (QueryParseException ex) {
				throw ex;
			} catch (Throwable th) {
				throw new IllegalStateException(th);
			}

		}

		return new Parse("AnchorParser", new ParseWithAnchor(field, anchors, aliases), overlay);
	}

	private String unquote(String value) {
		return value.substring(1, value.length() - 1);
	}

	private QueryCommand newParserFromRegistry(boolean overlay, String parserName) {
		if (registry.getProfile(parserName) == null) {
			// throw new QueryParseException("parser-not-found", -1);
			Map<String, String> params = new HashMap<String, String>();
			params.put("parser", parserName);
			params.put("value", parserName);
			throw new QueryParseException("21001", -1, -1, params);
		}
		try {
			return new Parse(parserName, registry.newParser(parserName), overlay);
		} catch (Throwable t) {
			// throw new QueryParseException("parser-init-failure", -1,
			// t.toString());
			Map<String, String> params = new HashMap<String, String>();
			params.put("parser", parserName);
			params.put("value", parserName);
			throw new QueryParseException("21002", -1, t.getMessage(), t);
		}
	}
}
