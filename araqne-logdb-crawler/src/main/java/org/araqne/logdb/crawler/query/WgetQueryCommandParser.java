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
package org.araqne.logdb.crawler.query;

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
import org.araqne.logdb.query.parser.ParseResult;
import org.araqne.logdb.query.parser.QueryTokenizer;

/**
 * @author xeraph@eediom.com
 */
@Component(name = "wget-query-command-parser")
public class WgetQueryCommandParser extends AbstractQueryCommandParser {
	@Requires
	private QueryParserService parserService;

	public WgetQueryCommandParser() {
		setDescriptions("Fetch HTTP content and parse DOM elements.", "HTTP 통신을 통해 웹페이지를 다운로드하고 해석한 결과를 출력합니다.");
		setOptions("url", false, "Request URL, starts with http:// or https://", "HTTP 요청할 URL을 입력합니다.");
		setOptions("selector", false, "DOM selector", "CSS의 셀렉터와 동일한 문법으로 HTML DOM 트리에서 선택할 요소를 지정합니다.");
		setOptions("timeout", false, "HTTP timeout. Default is 30 seconds.", "HTTP 연결 타임아웃 시간을 초 단위로 지정합니다. 미지정 시 기본값은 30초입니다.");
		setOptions("encoding", false, "Character encoding. Default is `utf-8`.",
				"HTTP 응답 해석에 사용할 인코딩을 지정합니다. 미지정 시 기본값은 utf-8 입니다.");
	}

	@Override
	public String getCommandName() {
		return "wget";
	}

	@Validate
	public void start() {
		parserService.addCommandParser(this);
	}

	@Invalidate
	public void stop() {
		if (parserService != null)
			parserService.removeCommandParser(this);
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(),
				Arrays.asList("selector", "timeout", "method", "encoding", "url", "auth"), getFunctionRegistry());

		@SuppressWarnings("unchecked")
		Map<String, String> options = (Map<String, String>) r.value;

		String url = options.get("url");
		String selector = options.get("selector");

		int timeout = 30;
		if (options.get("timeout") != null)
			timeout = Integer.valueOf(options.get("timeout"));

		String method = options.get("method");
		if (method == null)
			method = "get";
		else if (!method.equals("get") && !method.equals("post"))
			throw new QueryParseException("invalid-wget-method", -1);

		String encoding = options.get("encoding");
		if (encoding == null)
			encoding = "utf-8";

		String auth = options.get("auth");

		return new WgetQueryCommand(url, selector, timeout, method, encoding, auth);
	}
}
