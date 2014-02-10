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
package org.araqne.logdb.cep.query;

import java.util.Arrays;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryCommandParser;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.TimeSpan;
import org.araqne.logdb.cep.EventContextService;
import org.araqne.logdb.cep.EventContextStorage;
import org.araqne.logdb.query.expr.Expression;
import org.araqne.logdb.query.parser.ExpressionParser;
import org.araqne.logdb.query.parser.ParseResult;
import org.araqne.logdb.query.parser.QueryTokenizer;

@Component(name = "cepadd-parser")
public class CepAddParser implements QueryCommandParser {

	@Requires
	private QueryParserService queryParserService;

	@Requires
	private EventContextService eventContextService;

	@Override
	public String getCommandName() {
		return "cepadd";
	}

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
	public QueryCommand parse(QueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(),
				Arrays.asList("topic", "key", "expire", "timeout", "threshold", "maxrows"));

		@SuppressWarnings("unchecked")
		Map<String, String> options = (Map<String, String>) r.value;
		String topic = options.get("topic");
		String keyField = options.get("key");
		int maxRows = 10;
		try {
			if (options.get("maxrows") != null)
				maxRows = Integer.parseInt(options.get("maxrows").toString());
		} catch (NumberFormatException e) {
			throw new QueryParseException("invalid-maxrows", -1);
		}

		TimeSpan expire = null;
		if (options.get("expire") != null)
			expire = TimeSpan.parse(options.get("expire"));

		TimeSpan timeout = null;
		if (options.get("timeout") != null)
			timeout = TimeSpan.parse(options.get("timeout"));

		int threshold = 0;
		if (options.get("threshold") != null)
			threshold = Integer.valueOf(options.get("threshold"));

		Expression matcher = ExpressionParser.parse(context, commandString.substring(r.next));

		EventContextStorage storage = eventContextService.getStorage("mem");
		return new CepAddCommand(storage, topic, keyField, expire, timeout, threshold, maxRows, matcher);
	}
}
