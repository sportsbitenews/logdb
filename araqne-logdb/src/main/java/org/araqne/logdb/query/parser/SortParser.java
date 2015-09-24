/*
 * Copyright 2013 Future Systems
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.Sort;
import org.araqne.logdb.query.command.Sort.SortField;

public class SortParser extends AbstractQueryCommandParser {
	private static final String BY = "by";

	public SortParser() {
		setDescriptions(
				"Sort by specified comma-separated fields. If a hyphen is followed by field name, this command will sort by that field in descending order.",
				"주어진 필드를 기준으로 정렬합니다. 필드 이름 앞에 - 부호가 붙은 경우 내림차순, 그렇지 않으면 오름차순으로 정렬합니다. limit 옵션이 지정된 경우 정렬된 결과에서 순서대로 N개를 추출합니다.");
		setOptions("limit", OPTIONAL, "Max output count", "최대 출력 갯수");
	}

	@Override
	public String getCommandName() {
		return "sort";
	}

	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("21600", new QueryErrorMessage("need-column", "정렬할 필드명을 입력하십시오."));
		m.put("21601", new QueryErrorMessage("invalid-by-clause", "by절에 필드명들을 콤마로 구분하여 올바르게 입력하십시오. "));
		m.put("21602", new QueryErrorMessage("invalid-limit-option", "limit 값이 유효하지 않습니다. "));
		return m;
	}

	@Override
	@SuppressWarnings("unchecked")
	public QueryCommand parse(QueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, "sort".length(), Arrays.asList("limit"),
				getFunctionRegistry());
		Map<String, String> options = (Map<String, String>) r.value;

		Integer count = null;
		if (options.containsKey("limit")) {
			count = Integer.parseInt(options.get("limit"));
			if (count <= 0)
				throw new QueryParseException("21602", "sort".length() + 1, commandString.length() - 1, null);
		}

		List<String> partitions = new ArrayList<String>();
		int byPos = QueryTokenizer.findKeyword(commandString, BY, 0, true);
		if (byPos > 0) {
			String partitionsPart = commandString.substring(byPos + BY.length());

			if (partitionsPart.trim().endsWith(","))
				throw new QueryParseException("21601", "sort".length() + 1, commandString.length() - 1, null);

			// trim
			for (String partition : partitionsPart.split(",")) {
				partitions.add(partition.trim());
			}
		}

		try {
			List<SortField> fields = null;
			if (partitions.size() == 0)
				fields = SortField.parseSortFields(commandString, r);
			else
				fields = SortField.parseSortFields(commandString.substring(0, byPos), r);

			return new Sort(count, fields.toArray(new SortField[0]), partitions);
		} catch (QueryParseException e) {
			if (e.getType().equals("90004"))
				throw new QueryParseException("21600", r.next, commandString.length() - 1, null);
			throw e;
		}
	}
}
