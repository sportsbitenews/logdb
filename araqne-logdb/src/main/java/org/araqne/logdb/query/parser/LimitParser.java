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

import java.util.HashMap;
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.Limit;

/**
 * @since 1.7.2
 * @author xeraph
 * 
 */
public class LimitParser extends AbstractQueryCommandParser {

	public LimitParser() {
		setDescriptions(
				"Cancel query if input data count exceeds specified number. Some commands may not work properly if query is canceled.",
				"지정한 쿼리 입력 갯수에 도달하면 쿼리를 취소시킵니다. 일부 명령어의 경우 중간에 쿼리가 취소되면서 의도한대로 동작하지 않을 수 있으므로 주의해야 합니다.");
	}

	@Override
	public String getCommandName() {
		return "limit";
	}

	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("20600", new QueryErrorMessage("invalid-limit-args", "오프셋은 1개 또는 2개 입니다."));
		m.put("20601", new QueryErrorMessage("invalid-limit-arg-type:[msg]", "잘못된 오프셋 타입입니다: [msg]."));
		return m;
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		// commandString =
		// commandString.substring(getCommandName().length()).trim();
		String field = commandString.substring(getCommandName().length()).trim();
		String[] tokens = field.split(" ");
		if (tokens.length <= 0 || tokens.length > 2 || tokens[0].isEmpty()) {
			// throw new QueryParseException("invalid-limit-args", -1);
			throw new QueryParseException("20600", getCommandName().length() + 1, commandString.length() - 1, null);
		}

		try {
			if (tokens.length == 1) {
				long limit = Long.parseLong(tokens[0]);
				return new Limit(0, limit);
			} else {
				long offset = Long.parseLong(tokens[0]);
				long limit = Long.parseLong(tokens[1]);
				return new Limit(offset, limit);
			}
		} catch (NumberFormatException e) {

			// throw new QueryParseException("invalid-limit-arg-type", -1);
			Map<String, String> param = new HashMap<String, String>();
			param.put("msg", e.getMessage());
			throw new QueryParseException("20601", getCommandName().length() + 1, commandString.length() - 1, param);
		}
	}
}
