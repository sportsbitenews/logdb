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

import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryCommandParser;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.query.command.Limit;

/**
 * @since 1.7.2
 * @author xeraph
 * 
 */
public class LimitParser implements QueryCommandParser {

	@Override
	public String getCommandName() {
		return "limit";
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		commandString = commandString.substring(getCommandName().length()).trim();
		String[] tokens = commandString.split(" ");
		if (tokens.length <= 0 || tokens.length > 2 || tokens[0].isEmpty())
			throw new QueryParseException("invalid-limit-args", -1);

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
			throw new QueryParseException("invalid-limit-arg-type", -1);
		}
	}
}
