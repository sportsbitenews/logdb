/**
 * Copyright 2015 Eediom Inc.
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

import java.util.Arrays;
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.Repeat;

public class RepeatParser extends AbstractQueryCommandParser {

	public RepeatParser() {
		setDescriptions("Copy input data and write N times.", "입력 데이터를 지정된 횟수만큼 복사하여 출력합니다.");
		setOptions("count", true, "Repeat count", "반복 횟수");
	}

	@Override
	public String getCommandName() {
		return "repeat";
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(), Arrays.asList("count"),
				getFunctionRegistry());

		Map<String, String> options = (Map<String, String>) r.value;
		if (options.get("count") == null)
			throw new QueryParseException("missing-count-option", -1);

		int count = Integer.parseInt(options.get("count"));

		return new Repeat(count);
	}
}
