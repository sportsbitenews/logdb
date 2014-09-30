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
import java.util.List;
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.TimeSpan;
import org.araqne.logdb.TimeUnit;
import org.araqne.logdb.query.aggregator.AggregationField;
import org.araqne.logdb.query.command.Timechart;

public class TimechartParser extends AbstractQueryCommandParser {
	private static final String COMMAND = "timechart";
	private static final String BY = "by";

	@Override
	public String getCommandName() {
		return COMMAND;
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		// timechart <options> <aggregation functions> by <clause>
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, COMMAND.length(), Arrays.asList("span"),
				getFunctionRegistry());

		@SuppressWarnings("unchecked")
		Map<String, String> options = (Map<String, String>) r.value;

		String argsPart = commandString.substring(r.next);
		String aggsPart = argsPart;

		List<String> clauses = new ArrayList<String>();

		// parse clauses
		int byPos = QueryTokenizer.findKeyword(argsPart, BY, 0, true);
		if (byPos > 0) {
			aggsPart = argsPart.substring(0, byPos);
			String clausePart = argsPart.substring(byPos + BY.length());
			clauses = Arrays.asList(clausePart.split(","));
		}

		// parse aggregations
		List<String> aggTerms = QueryTokenizer.parseByComma(aggsPart);
		List<AggregationField> fields = new ArrayList<AggregationField>();

		for (String aggTerm : aggTerms) {
			AggregationField field = AggregationParser.parse(context, aggTerm, getFunctionRegistry());
			fields.add(field);
		}

		if (fields.size() == 0)
		//	throw new QueryParseException("need-aggregation-field", COMMAND.length());
			throw new QueryParseException("21800", COMMAND.length() + 1, commandString.length() - 1, null);
		
		// parse timespan option
		TimeSpan timeSpan = null;
		if (options.containsKey("span"))
			timeSpan = TimeSpan.parse(options.get("span"));

		if (timeSpan == null)
			timeSpan = new TimeSpan(1, TimeUnit.Day);

		String clause = null;
		if (!clauses.isEmpty())
			clause = clauses.get(0).trim();

		return new Timechart(fields, clause, timeSpan);
	}
}
