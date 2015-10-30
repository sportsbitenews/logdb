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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.Rename;

public class RenameParser extends AbstractQueryCommandParser {

	public RenameParser() {
		setDescriptions("Rename field name. For example, `rename A as B` means A will be renamed to B.",
				"원본 필드 이름을 as 뒤에 지정된 필드 이름으로 변경합니다.");
	}

	@Override
	public String getCommandName() {
		return "rename";
	}

	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("20800", new QueryErrorMessage("as-token-not-found", "원본 필드를 입력하십시오."));
		m.put("20801", new QueryErrorMessage("to-field-not-found", "변경 필드를 입력하십시오."));
		m.put("20802", new QueryErrorMessage("invalid-as-position", "잘못된 문법:  [as] 자리에 as가 와야 합니다."));
		return m;
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		List<Rename.Pair> pairs = new ArrayList<Rename.Pair>();

		int s = getCommandName().length();

		while (true) {
			int e = commandString.indexOf(',', s);
			String tokenString = (e < 0) ? commandString.substring(s) : commandString.substring(s, e);
			QueryTokens tokens = QueryTokenizer.tokenize(tokenString);
			if (tokens.size() < 1)
				// throw new QueryParseException("as-token-not-found",
				// tokenString.length());
				throw new QueryParseException("20800", s + 1, s + tokenString.length() - 1, null);

			if (tokens.size() < 3)
				// throw new QueryParseException("to-field-not-found",
				// tokenString.length());
				throw new QueryParseException("20801", s + QueryTokenizer.findIndexOffset(tokens, 0),
						s + tokenString.length() - 1, null);

			if (!tokens.string(1).equalsIgnoreCase("as")) {
				// throw new QueryParseException("invalid-as-position", -1);
				String AS = tokens.string(1);
				Map<String, String> params = new HashMap<String, String>();
				params.put("AS", AS);
				int offset = QueryTokenizer.findIndexOffset(tokens, 1);
				throw new QueryParseException("20802", s + offset, s + offset + AS.length() - 1, params);
			}

			String from = tokens.string(0);
			String to = tokens.string(2);
			pairs.add(new Rename.Pair(from, to));

			if (e < 0)
				break;

			s = e + 1;
		}
		return new Rename(pairs);
	}
}
