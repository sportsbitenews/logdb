/*
 * Copyright 2013 Future Systems
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.Rex;

public class RexParser extends AbstractQueryCommandParser {

	public RexParser() {
		setDescriptions(
				"Extract fields from target field using regular expression. You use (?<name>.*) syntax to extract 'name' field.",
				"지정된 필드에서 정규표현식을 이용하여 필드를 추출합니다. 이 때 사용되는 정규표현식은 필드 이름을 부여할 수 있도록 확장된 정규표현식입니다. "
						+ "정규표현식 그룹을 만들 때 (?<field>.*) 형식으로 지정하면 그룹에 매칭된 문자열이 field 이름으로 추출됩니다.");

		setOptions("field", REQUIRED, "Target field name", "추출 대상 필드 이름");
	}

	@Override
	public String getCommandName() {
		return "rex";
	}

	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("20900", new QueryErrorMessage("field-not-found", "filed 옵션의 값이 없습니다."));
		m.put("20901", new QueryErrorMessage("invalid-regex", "올바르지 않는 정규표현식입니다."));
		return m;
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {

		// extract field names and remove placeholder
		List<String> names = new ArrayList<String>();

		ParseResult r = QueryTokenizer.parseOptions(context, commandString, "rex".length(), Arrays.asList("field"),
				getFunctionRegistry());
		@SuppressWarnings("unchecked")
		Map<String, String> options = (Map<String, String>) r.value;

		String field = options.get("field");
		if (field == null || field.isEmpty()) {
			// throw new QueryParseException("field-not-found",
			// commandString.length());
			int offset = QueryTokenizer.findKeyword(commandString, "=") + 1;
			throw new QueryParseException("20900", offset, offset, null);
		}
		Pattern placeholder = Pattern.compile("\\(\\?<(.*?)>");
		String regexToken = commandString.substring(r.next);
		if (!QueryTokenizer.isQuoted(regexToken.trim()))
			throw new QueryParseException("20901", r.next, commandString.length() - 1, null);

		// for later toString convenience
		String originalRegexToken = regexToken;

		regexToken = QueryTokenizer.removeQuotes(regexToken);
		regexToken = toNonCapturingGroup(regexToken);

		Matcher matcher = placeholder.matcher(regexToken);
		while (matcher.find()) {
			names.add(matcher.group(1));
		}

		while (true) {
			matcher = placeholder.matcher(regexToken);
			if (!matcher.find())
				break;

			// suppress special meaning of $ and \
			String quoted = Matcher.quoteReplacement("(");
			regexToken = matcher.replaceFirst(quoted);
		}

		Pattern p = Pattern.compile(regexToken);
		return new Rex(field, originalRegexToken, p, names.toArray(new String[0]));
	}

	private String toNonCapturingGroup(String s) {
		StringBuilder sb = new StringBuilder();

		char last2 = ' ';
		char last = ' ';
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			if (last2 != '\\' && last == '(' && c != '?')
				sb.append("?:");
			sb.append(c);
			last2 = last;
			last = c;
		}

		return sb.toString();
	}

}
