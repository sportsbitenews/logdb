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

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.Mv;

/**
 * @since 2.0.2-SNAPSHOT
 * @author darkluster
 * 
 */
public class MvParser extends AbstractQueryCommandParser {

	public MvParser() {
		setDescriptions("Move file when query completes", "쿼리 완료 시 지정된 파일을 이동합니다.");
		setOptions("from", REQUIRED, "Original file path", "원본 파일 경로");
		setOptions("to", REQUIRED, "Target file path", "이동할 경로");
	}

	@Override
	public String getCommandName() {
		return "mv";
	}

	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("30500", new QueryErrorMessage("missing-field", "잘못된 쿼리문 입니다."));
		m.put("30501", new QueryErrorMessage("missing-field", "from 또는 to 옵션값이 없습니다."));
		m.put("30502", new QueryErrorMessage("file-exists", "[file] 파일이 이미 존재합니다."));
		return m;
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		if (commandString.trim().endsWith(","))
			// throw new QueryParseException("missing-field",
			// commandString.length());
			throw new QueryParseException("30500", commandString.trim().length() - 1, commandString.trim().length() - 1, null);

		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(),
				Arrays.asList("from", "to"), getFunctionRegistry());
		Map<String, String> options = (Map<String, String>) r.value;
		if (!options.containsKey("from") || !options.containsKey("to"))
			// throw new QueryParseException("missing-field",
			// commandString.length());
			throw new QueryParseException("30501", getCommandName().length() + 1, commandString.length() - 1, null);

		String from = options.get("from");
		String to = options.get("to");

		if (new File(to).exists()) {
			// throw new QueryParseException("file-exists", -1, to);
			Map<String, String> params = new HashMap<String, String>();
			params.put("file", to);
			int offset = QueryTokenizer.findKeyword(commandString, to, QueryTokenizer.findIndexOffset(commandString, 2));
			throw new QueryParseException("30502", offset, offset + to.length() - 1, params);
		}
		return new Mv(from, to);
	}
}
