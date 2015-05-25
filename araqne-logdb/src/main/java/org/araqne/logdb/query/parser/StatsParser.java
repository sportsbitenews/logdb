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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.aggregator.AggregationField;
import org.araqne.logdb.query.command.Stats;

public class StatsParser extends AbstractQueryCommandParser {
	private static final String BY = "by";

	@Override
	public String getCommandName() {
		return "stats";
	}

	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("21700", new QueryErrorMessage("missing-stats-function", "집계 함수를 입력하세요."));
		m.put("21701", new QueryErrorMessage("missing-clause", "잘못된 쿼리 입니다. "));
		m.put("21702", new QueryErrorMessage("invalid-aggregation-function", "[function]은 지원하지 않는 함수입니다."));
		m.put("21703", new QueryErrorMessage("cannot-create-aggregation-function", "[function] 함수를 생성할 수 없습니다. [msg] "));
		return m;
	}
	
	public static class SyntaxParseResult {
		public SyntaxParseResult(List<String> clauses, List<String> aggTerms) {
			this.clauses = clauses;
			this.aggTerms = aggTerms;
		}

		public List<String> clauses;
		public List<String> aggTerms;
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		SyntaxParseResult pr = parseSyntax(context, commandString);

		// parse aggregations
		List<AggregationField> fields = new ArrayList<AggregationField>();

		for (String aggTerm : pr.aggTerms) {
			AggregationField field;
			try {
				field = AggregationParser.parse(context, aggTerm, getFunctionRegistry());
			} catch (QueryParseException e) {
				Map<String, String> params = e.getParams();
				if(params == null || !params.containsKey("value") || params.get("value") == null)
					throw new QueryParseException(e.getType(), getCommandName().length() + 1, commandString.length() -1, params);

				String subQuery = params.get("value");
				int offset = commandString.indexOf(subQuery);
				throw new QueryParseException(e.getType(), offset, offset + subQuery.length() -1 , params);
			} 
			fields.add(field);
		}

		if (fields.isEmpty())
			//	throw new QueryParseException("missing-stats-function", -1);
			throw new QueryParseException("21700", getCommandName().length() + 1, commandString.length() -1 ,null);

		return new Stats(fields, pr.clauses);
	} 

	public SyntaxParseResult parseSyntax(QueryContext context, String commandString) {
		// stats <aggregation function holder> by <stats-fields>

		List<String> clauses = new ArrayList<String>();
		String aggsPart = commandString.substring(getCommandName().length());

		// parse clauses
		int byPos = QueryTokenizer.findKeyword(commandString, BY, 0, true);
		if (byPos > 0) {
			aggsPart = commandString.substring(getCommandName().length(), byPos);
			String clausePart = commandString.substring(byPos + BY.length());

			if (clausePart.trim().endsWith(","))
				//	throw new QueryParseException("missing-clause", commandString.length());
				throw new QueryParseException("21701", getCommandName().length() + 1, commandString.length() -1 ,null);

			// trim
			for (String clause : clausePart.split(","))
				clauses.add(clause.trim());
		}

		// parse aggregations
		List<String> aggTerms = QueryTokenizer.parseByComma(aggsPart);

		return new SyntaxParseResult(clauses, aggTerms);
	}

}
