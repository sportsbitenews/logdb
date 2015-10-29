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
package org.araqne.logdb.query.parser;

import java.util.Arrays;
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.query.command.ParseJson;

public class ParseJsonParser extends AbstractQueryCommandParser {

	public ParseJsonParser() {
		setDescriptions("Parse JSON literal to tuple.", "JSON 문자열을 파싱하여 레코드로 변환합니다.");
		setOptions("overlay", false, "Use `overlay=t` option if you want to override parsed fields on original data.",
				"t로 주면, 원본 필드에 JSON에서 추출된 필드를 덧씌운 결과를 출력으로 내보냅니다. 별도로 overlay 옵션을 지정하지 않으면, JSON 문자열을 파싱한 결과만 출력으로 내보냅니다.");
		setOptions("field", false, "Specify target field name. Default value is `line`.", "대상 필드를 별도로 지정하지 않는 경우 기본값은 line입니다.");
	}

	@Override
	public String getCommandName() {
		return "parsejson";
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(),
				Arrays.asList("field", "overlay"), getFunctionRegistry());

		Map<String, String> options = (Map<String, String>) r.value;
		String field = options.get("field");
		if (field == null)
			field = "line";

		boolean overlay = CommandOptions.parseBoolean(options.get("overlay"));
		return new ParseJson(field, overlay);
	}

}
