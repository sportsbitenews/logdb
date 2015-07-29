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
import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.cep.EventContextService;
import org.araqne.logdb.cep.EventContextStorage;
import org.araqne.logdb.query.parser.CommandOptions;
import org.araqne.logdb.query.parser.ParseResult;
import org.araqne.logdb.query.parser.QueryTokenizer;

@Component(name = "evtctxdrop-parser")
public class EvtCtxDropParser extends AbstractQueryCommandParser {
	@Requires
	private QueryParserService queryParserService;

	@Requires
	private EventContextService eventContextService;

	public EvtCtxDropParser() {
		setDescriptions("Delete all event contexts by event topic", "지정된 주제에 해당하는 모든 이벤트 컨텍스트를 일괄 삭제합니다.");

		setOptions("all", OPTIONAL, "To delete all event contexts, use 't' as value.", "t로 지정 시, 모든 이벤트 컨텍스트를 일괄적으로 삭제합니다.");
		setOptions("topic", OPTIONAL, "Delete all event contexts by topic", "지정된 주제를 가진 모든 이벤트 컨텍스트를 삭제합니다.");
	}

	@Override
	public String getCommandName() {
		return "evtctxdrop";
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
				Arrays.asList("topic", "all"), getFunctionRegistry());

		@SuppressWarnings("unchecked")
		Map<String, String> options = (Map<String, String>) r.value;
		boolean dropAll = CommandOptions.parseBoolean(options.get("all"));
		String topic = options.get("topic");
		if (!dropAll && topic == null)
			throw new QueryParseException("missing-evtctxdrop-topic", -1);

		EventContextStorage storage = eventContextService.getStorage("mem");
		return new EvtCtxDropCommand(storage, topic, dropAll);
	}

}
