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
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.cep.EventContextService;
import org.araqne.logdb.cep.EventContextStorage;
import org.araqne.logdb.query.expr.Expression;
import org.araqne.logdb.query.parser.ExpressionParser;
import org.araqne.logdb.query.parser.ParseResult;
import org.araqne.logdb.query.parser.QueryTokenizer;

@Component(name = "evtctxdel-parser")
public class EvtCtxDelParser extends AbstractQueryCommandParser {
	@Requires
	private QueryParserService queryParserService;

	@Requires
	private EventContextService eventContextService;

	public EvtCtxDelParser() {
		setDescriptions("Remove event context if input tuple matches the specified expression",
				"입력 데이터가 조건식과 일치하는 경우 주어진 키로 이벤트 컨텍스트를 삭제합니다.");

		setOptions("topic", REQUIRED, "Event topic namespace", "이벤트 토픽. 각 이벤트 컨텍스트 이름 공간을 구분합니다.");
		setOptions("key", REQUIRED, "Field name for event context key", "키 필드 이름. 이벤트 컨텍스트를 구분하는 키 값을 추출할 필드를 입력합니다.");
	}

	@Override
	public String getCommandName() {
		return "evtctxdel";
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
				Arrays.asList("topic", "key", "logtick"), getFunctionRegistry());

		@SuppressWarnings("unchecked")
		Map<String, String> options = (Map<String, String>) r.value;
		String topic = options.get("topic");
		String keyField = options.get("key");
		String hostField = options.get("logtick");

		Expression matcher = ExpressionParser.parse(context, commandString.substring(r.next), getFunctionRegistry());

		String engine = System.getProperty("araqne.logdb.cepengine");
		EventContextStorage storage = eventContextService.getStorage(engine);
		return new EvtCtxDelCommand(storage, topic, keyField, matcher, hostField);
	}

}
