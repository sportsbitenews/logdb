/*
 * Copyright 2014 Eediom Inc.
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

import java.util.HashMap;
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.Explode;

public class ExplodeParser extends AbstractQueryCommandParser {

	@Override
	public String getCommandName() {
		return "explode";
	}

	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("20300", new QueryErrorMessage("missing-explode-field","올바르지 않는 필드 이름입니다."));
		return m;
	}
	
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		String arrayFieldName = commandString.substring(getCommandName().length()).trim();
		if (arrayFieldName.isEmpty())
			//throw new QueryParseException("missing-explode-field", -1);.
			throw new QueryParseException("20300", getCommandName().length()  + 1,  commandString.length() - 1, null);

		return new Explode(arrayFieldName);
	}
}
