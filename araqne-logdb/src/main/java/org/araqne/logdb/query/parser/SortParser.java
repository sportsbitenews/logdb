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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.LogQueryCommand;
import org.araqne.logdb.LogQueryCommandParser;
import org.araqne.logdb.LogQueryContext;
import org.araqne.logdb.LogQueryParseException;
import org.araqne.logdb.query.command.Sort;
import org.araqne.logdb.query.command.Sort.SortField;

public class SortParser implements LogQueryCommandParser {

	@Override
	public String getCommandName() {
		return "sort";
	}

	@Override
	@SuppressWarnings("unchecked")
	public LogQueryCommand parse(LogQueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, "sort".length(), Arrays.asList("limit"));
		Map<String, String> options = (Map<String, String>) r.value;

		Integer count = null;
		if (options.containsKey("limit"))
			count = Integer.parseInt(options.get("limit"));

		try {
			List<SortField> fields = SortField.parseSortFields(commandString, r);
			return new Sort(count, fields.toArray(new SortField[0]));
		} catch (LogQueryParseException e) {
			if (e.getType().equals("need-string-token"))
				throw new LogQueryParseException("need-column", r.next);
			throw e;
		}
	}
}
