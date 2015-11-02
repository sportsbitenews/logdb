/**
 * Copyright 2015 Eediom Inc.
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
import org.araqne.logdb.query.command.ParseXml;

/**
 * @author xeraph
 */
public class ParseXmlParser extends AbstractQueryCommandParser {

	public ParseXmlParser() {
		setDescriptions(
				"Parse XML literal to tuple. Direct children elements of root node are extracted as fields. If XML element has only text literal, extracted value will be string. If XML element has any attribute, extracted value will be map which has attribute key/value pairs.\n\n"
						+ "For example, if you parse `<doc><id>sample</id></doc>` string literal, parsexml will output tuple which has `id` field with `sample` string value. However, the value of name field will be map which has `locale` and `_text` keys for `<doc><id>sample</id><name locale=\\\"ko\\\">description</name></doc>`.",
				"XML 문서를 복합 객체의 집합으로 파싱합니다. 루트 XML 엘리먼트에 속한 자식 XML 엘리먼트는 필드로 추출됩니다. 파싱 시 XML 엘리먼트가 문자열만 포함한다면 필드 값으로 문자열을 할당하지만, XML 어트리뷰트가 존재한다면 맵을 할당하고 XML 엘리먼트의 텍스트 내용을 _text 키로 저장하며, 각 XML 어트리뷰트 이름/값 쌍을 맵의 키/값 쌍으로 저장합니다.\n\n"
						+ "예를 들어, <doc><id>sample</id></doc> 형태의 XML을 parsexml로 파싱한다면, id 필드에 sample 문자열 값이 할당됩니다. 그러나 <doc><id>sample</id><name locale=\"ko\">로그프레소</name></doc> 형태의 XML이라면 name 필드에는 locale=ko, _text=로그프레소 2개의 키/값을 포함한 맵이 할당됩니다. 이후에 설명할 parsemap 명령어를 조합하면 복합 객체 안에 있는 맵에서 쉽게 필드를 추출할 수 있습니다.");

		setOptions("overlay", false, "Use `overlay=t` option if you want to override parsed fields on original data.",
				"t로 주면, 원본 필드에 XML에서 추출된 필드를 덧씌운 결과를 출력으로 내보냅니다. 별도로 overlay 옵션을 지정하지 않으면, XML 문자열을 파싱한 결과만 출력으로 내보냅니다.");
		setOptions("field", false, "Specify target field name. Default value is `line`.", "대상 필드를 별도로 지정하지 않는 경우 기본값은 line입니다.");
	}

	@Override
	public String getCommandName() {
		return "parsexml";
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
		return new ParseXml(field, overlay);
	}
}
