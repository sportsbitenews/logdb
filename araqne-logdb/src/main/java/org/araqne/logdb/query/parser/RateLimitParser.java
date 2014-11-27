/**
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

import org.araqne.cron.TickService;
import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.RateLimit;

public class RateLimitParser extends AbstractQueryCommandParser {

	private TickService tickService;

	public RateLimitParser(TickService tickService) {
		this.tickService = tickService;
	}

	@Override
	public String getCommandName() {
		return "ratelimit";
	}
	
	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("22200", new QueryErrorMessage("ratelimit-should-be-positive","ratelimit 값[rate]은 0보다 커야 합니다."));
		m.put("22201", new QueryErrorMessage("invalid-ratelimit-number", "ratelimit 값[rate]은 숫자여야 합니다.")); 
		return m;
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		try {
			int limit = Integer.valueOf(commandString.substring(getCommandName().length()).trim());
			if (limit <= 0){
			//	throw new QueryParseException("ratelimit-should-be-positive", -1);
				Map<String, String> params = new HashMap<String, String >();
				params.put("rate", limit +"" );
				throw new QueryParseException("22200", getCommandName().length()  + 1,  commandString.length() - 1, null);
			}

			return new RateLimit(tickService, limit);
		} catch (NumberFormatException e) {
			//throw new QueryParseException("invalid-ratelimit-number", -1);
			Map<String, String> params = new HashMap<String, String> ();
			params.put("rate", commandString.substring(getCommandName().length()).trim() );
			throw new QueryParseException("22201", getCommandName().length()  + 1,  commandString.length() - 1, null);
		}
	}
}
