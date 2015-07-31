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
import java.util.HashMap;
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.Query;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryService;
import org.araqne.logdb.Session;
import org.araqne.logdb.query.command.Result;

public class ResultParser extends AbstractQueryCommandParser {

	private QueryService queryService;

	public ResultParser(QueryService queryService) {
		this.queryService = queryService;
	}

	@Override
	public String getCommandName() {
		return "result";
	}

	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("11500", new QueryErrorMessage("Query not found", "지정된 쿼리가 존재하지 않습니다."));
		return m;
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(),
				Arrays.asList("offset", "limit"), getFunctionRegistry());
		@SuppressWarnings("unchecked")
		Map<String, String> options = (Map<String, String>) r.value;

		long offset = 0;
		if (options.containsKey("offset"))
			offset = Long.parseLong(options.get("offset"));

		long limit = 0;
		if (options.containsKey("limit"))
			limit = Long.parseLong(options.get("limit"));

		String token = commandString.substring(r.next);
		int queryId = Integer.valueOf(token);

		Query query = queryService.getQuery(queryId);
		if (query == null) {
			throw new QueryParseException("11500", -1);
		}

		// check access control
		Session currentSession = context.getSession();
		if (currentSession != null && !currentSession.isAdmin()) {
			Session querySession = query.getContext().getSession();

			// other session should not know specific query id exists
			if (querySession == null || !querySession.getLoginName().equals(currentSession.getLoginName())) {
				throw new QueryParseException("11500", -1);
			}
		}

		return new Result(query, offset, limit);
	}

}
