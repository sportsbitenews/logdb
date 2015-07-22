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
import org.araqne.logdb.query.command.Fields;

public class FieldsParser extends AbstractQueryCommandParser {

	@Override
	public String getCommandName() {
		return "fields";
	}
	
	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("20400", new QueryErrorMessage("no-field-args","필드 이름이 없습니다. "));
		return m;
	}

	
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		QueryTokens tokens = QueryTokenizer.tokenize(commandString);

		List<String> fields = new ArrayList<String>();
		List<String> args = tokens.substrings(1);

		
		boolean selector = true;
		if (args.get(0).equals("-")) {
			selector = false;
			args.remove(0);
		}
		
		if (args.size() == 0)
			//throw new QueryParseException("no-field-args", -1);
			throw new QueryParseException("20400", getCommandName().length()  + 1,  commandString.length() - 1, null);


		for (String t : args) {
			String[] csv = t.split(",");
			for (String s : csv) {
				fields.add(s.trim());
			}
		}

		return new Fields(fields, selector);
	}
}
