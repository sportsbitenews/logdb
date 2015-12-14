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

import java.util.List;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.query.command.Union;

/**
 * @since 2.2.13
 * @author xeraph
 * 
 */
public class UnionParser extends AbstractQueryCommandParser {

	private QueryParserService queryParserService;

	public UnionParser(QueryParserService queryParserService) {
		this.queryParserService = queryParserService;
		setDescriptions("Merge result set of sub-query. `union` runs in parallel, and do not guarantee order.",
				"서브 쿼리의 결과를 병합합니다. union은 다른 쿼리와 병행하여 실행되므로 출력 순서를 보장하지 않습니다. 통계 처리를 수행하는 경우처럼, 순서가 중요하지 않으면서 높은 수행 성능이 필요할 때에 주로 사용합니다.");
	}

	@Override
	public String getCommandName() {
		return "union";
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		int b = commandString.indexOf('[');
		int e = commandString.lastIndexOf(']');

		QueryContext subQueryContext = new QueryContext(context.getSession(), context);
		String subQueryString = commandString.substring(b + 1, e).trim();
		List<QueryCommand> commands = queryParserService.parseCommands(subQueryContext, subQueryString);
		return new Union(context, commands);
	}
}
