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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.query.command.Prev;

public class PrevParser extends AbstractQueryCommandParser {

	public PrevParser() {
		setDescriptions("Assign previous field value to `prev_FIELD` field.", "`prev_필드이름`으로 이전 행의 필드 값을 할당합니다.");
		setUsages("prev total", "prev total");
	}

	@Override
	public String getCommandName() {
		return "prev";
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		QueryTokens tokens = QueryTokenizer.tokenize(commandString);

		Set<String> fields = new HashSet<String>();
		List<String> args = tokens.substrings(1);

		for (String t : args) {
			String[] csv = t.split(",");
			for (String s : csv) {
				s = s.trim();
				if (!s.isEmpty())
					fields.add(s);
			}
		}

		return new Prev(fields.toArray(new String[0]));
	}

}
