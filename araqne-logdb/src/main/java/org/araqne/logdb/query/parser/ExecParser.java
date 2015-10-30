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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.TimeSpan;
import org.araqne.logdb.query.command.Exec;
import org.araqne.logdb.query.expr.Expression;

/**
 * @since 2.4.24
 * @author xeraph
 * 
 */
public class ExecParser extends AbstractQueryCommandParser {
	public ExecParser() {
		setDescriptions("Execute external program and read standard output.",
				"임의의 시스템 명령을 실행하여 표준출력을 조회합니다. 관리자 권한이 없으면 쿼리가 실패합니다.");

		setOptions("timeout", false,
				"Process timeout. You should use s(second), m(minute), h(hour), d(day), mon(month) time unit. For example, `10s` means wait 10 seconds and kill process after timeout.",
				"프로세스 종료 대기 타임아웃. s(초), m(분), h(시), d(일), mon(월) 단위로 지정할 수 있습니다. 예를 들면, 10s의 경우 10초 안에 프로세스가 종료되지 않으면 프로세스를 강제 종료합니다.");
	}

	@Override
	public String getCommandName() {
		return "exec";
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		if (context != null && !context.getSession().isAdmin())
			throw new QueryParseException("no-permission", -1);

		ParseResult r = QueryTokenizer.parseOptions(context, commandString, 5, Arrays.asList("timeout"), getFunctionRegistry());
		Map<String, String> m = (Map<String, String>) r.value;

		TimeSpan timeout = null;
		if (m.get("timeout") != null)
			timeout = TimeSpan.parse(m.get("timeout").toString());

		r = QueryTokenizer.nextString(commandString, r.next);
		if (r == null)
			throw new QueryParseException("missing-exec-path", -1);

		String command = r.value.toString();

		List<Expression> args = new ArrayList<Expression>();
		for (String token : QueryTokenizer.parseByComma(commandString.substring(r.next))) {
			if (token.trim().isEmpty())
				throw new QueryParseException("missing-token", r.next);
			Expression arg = ExpressionParser.parse(context, token, getFunctionRegistry());
			args.add(arg);
		}

		return new Exec(command, args, timeout);
	}
}
