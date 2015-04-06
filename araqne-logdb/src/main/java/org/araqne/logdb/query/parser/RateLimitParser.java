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
import java.util.List;
import java.util.Map;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.araqne.cron.TickService;
import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.RateLimit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	
	private static Logger logger = LoggerFactory.getLogger(RateLimitParser.class);

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		boolean splitRowBatch = false;
		int delay = 1000;
		try {
			OptionParser parser = new OptionParser("1d:");
			OptionSet opts = parser.parse(ExpressionParser.translateCommandline(commandString));
			List<?> argl = opts.nonOptionArguments();
			commandString = (String) argl.get(1); // get(0) == "ratelimit"
			if (opts.has("1"))
				splitRowBatch = true;
			if (opts.has("d") && opts.hasArgument("d"))
				delay = Integer.parseInt((String) opts.valueOf("d"));
		} catch (Throwable t) {
			logger.warn(commandString, t);
		}
		try {
			int limit = Integer.valueOf(commandString);
			if (limit <= 0){
			//	throw new QueryParseException("ratelimit-should-be-positive", -1);
				Map<String, String> params = new HashMap<String, String >();
				params.put("rate", limit +"" );
				throw new QueryParseException("22200", getCommandName().length()  + 1,  commandString.length() - 1, null);
			}

			return new RateLimit(tickService, limit, splitRowBatch, delay);
		} catch (NumberFormatException e) {
			//throw new QueryParseException("invalid-ratelimit-number", -1);
			Map<String, String> params = new HashMap<String, String> ();
			params.put("rate", commandString.substring(getCommandName().length()).trim() );
			throw new QueryParseException("22201", getCommandName().length()  + 1,  commandString.length() - 1, null);
		}
	}
}
