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
import org.araqne.logdb.TimeSpan;
import org.araqne.logdb.cep.EventContextService;
import org.araqne.logdb.cep.EventContextStorage;
import org.araqne.logdb.query.expr.Expression;
import org.araqne.logdb.query.parser.ExpressionParser;
import org.araqne.logdb.query.parser.ParseResult;
import org.araqne.logdb.query.parser.QueryTokenizer;

@Component(name = "evtctxadd-parser")
public class EvtCtxAddParser extends AbstractQueryCommandParser {

	@Requires
	private QueryParserService queryParserService;

	@Requires
	private EventContextService eventContextService;

	public EvtCtxAddParser() {
		setDescriptions(
				"Create an event context if input tuple matches specified expression. If event context already exists then timeout is extended. Expiry time is not extended.",
				"입력 데이터가 조건식과 일치하는 경우 주어진 키로 이벤트 컨텍스트를 생성합니다. 이벤트 컨텍스트에 타임아웃이 설정된 상태에서 조건식에 일치하는 입력 데이터가 추가로 들어오면, 타임아웃이 연장됩니다. 이 때 만료 시간은 연장되지 않습니다.");

		setOptions("topic", REQUIRED, "Event topic namespace", "다른 규칙의 이벤트와 구분이 되도록 유일한 이름을 부여합니다.");
		setOptions("key", REQUIRED, "Field name for event context key", "이벤트 컨텍스트를 구분하는 키 값을 추출할 필드를 입력합니다.");
		setOptions("expire", OPTIONAL, "Event context will be removed in specified expiry time. "
				+ "You can use s(second), m(minute), h(hour), d(day), mon(month) as time unit.",
				"이벤트 컨텍스트 생성 시점부터 지정된 만료 시간 후에 이벤트 컨텍스트가 삭제됩니다. s(초), m(분), h(시), d(일), mon(월) 단위로 지정할 수 있습니다.");
		setOptions("timeout", OPTIONAL, "Event context will be removed in specified timeout time. "
				+ "You can use s(second), m(minute), h(hour), d(day), mon(month) as time unit.",
				"마지막으로 매칭된 이벤트 수신 시점으로부터 타임아웃 시간 후에 이벤트 컨텍스트가 삭제됩니다. s(초), m(분), h(시), d(일), mon(월) 단위로 지정할 수 있습니다.");
		setOptions("maxrows", OPTIONAL, "Max tuple count for an event context. Default is 10.",
				"이벤트 컨텍스트에 저장할 최대 행 갯수를 지정합니다. 미지정 시 10으로 설정됩니다.");
	}

	@Override
	public String getCommandName() {
		return "evtctxadd";
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
				Arrays.asList("topic", "key", "expire", "timeout", "maxrows", "logtick"), getFunctionRegistry());

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

		String hostField = options.get("logtick");

		Expression matcher = ExpressionParser.parse(context, commandString.substring(r.next), getFunctionRegistry());

		EventContextStorage storage = eventContextService.getDefaultStorage();
		return new EvtCtxAddCommand(storage, topic, keyField, expire, timeout, maxRows, matcher, hostField);
	}
}
