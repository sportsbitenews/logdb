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
package org.araqne.logdb.jms.query;

import java.util.Arrays;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.jms.JmsProfile;
import org.araqne.logdb.jms.JmsProfileRegistry;
import org.araqne.logdb.query.parser.ParseResult;
import org.araqne.logdb.query.parser.QueryTokenizer;

@Component(name = "jms-send-parser")
public class JmsSendCommandParser extends AbstractQueryCommandParser {

	@Requires
	private QueryParserService queryParserService;

	@Requires
	private JmsProfileRegistry profileRegistry;

	@Validate
	public void start() {
		queryParserService.addCommandParser(this);
	}

	@Invalidate
	public void stop() {
		if (queryParserService != null)
			queryParserService.removeCommandParser(this);
	}

	@Override
	public String getCommandName() {
		return "jmssend";
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		String cmd = commandString.substring(getCommandName().length()).trim();
		int p = cmd.indexOf(' ');

		String name = null;
		if (p < 0) {
			name = cmd.trim();
		} else {
			name = cmd.substring(0, p);
			cmd = cmd.substring(p + 1);
		}

		ParseResult r = QueryTokenizer.parseOptions(context, cmd, 0, Arrays.asList("type"),
				queryParserService.getFunctionRegistry());

		@SuppressWarnings("unchecked")
		Map<String, Object> m = (Map<String, Object>) r.value;

		JmsProfile profile = profileRegistry.getProfile(name);
		if (profile == null)
			throw new QueryParseException("jms-profile-not-found", -1);

		return new JmsSendCommand(profile);
	}

}
