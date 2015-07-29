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

	public ExplodeParser() {
		setDescriptions(
				"Generate tuples for each element in specified array. You can pivot horizontal data to vertical (row becomes column). "
						+ "If specified field does not exist, or is not array, or is null, then original tuple will be passed.",
				"지정된 배열의 각 원소마다 대응되는 행을 생성합니다. 일반적으로 배열(가로)을 열(세로) 방향으로 축 변환하려는 경우에 사용합니다. "
						+ "지정된 필드가 존재하지 않거나, 배열이 아니거나, null인 경우 입력 행을 보존합니다.");
	}

	@Override
	public String getCommandName() {
		return "explode";
	}

	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("20300", new QueryErrorMessage("missing-explode-field", "올바르지 않는 필드 이름입니다."));
		return m;
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		String arrayFieldName = commandString.substring(getCommandName().length()).trim();
		if (arrayFieldName.isEmpty())
			// throw new QueryParseException("missing-explode-field", -1);.
			throw new QueryParseException("20300", getCommandName().length() + 1, commandString.length() - 1, null);

		return new Explode(arrayFieldName);
	}
}
