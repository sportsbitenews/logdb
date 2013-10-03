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

import org.araqne.logdb.LogQueryCommand;
import org.araqne.logdb.LogQueryCommandParser;
import org.araqne.logdb.LogQueryContext;
import org.araqne.logdb.LogQueryParseException;
import org.araqne.logdb.query.aggregator.AggregationField;
import org.araqne.logdb.query.command.Timechart;
import org.araqne.logdb.query.command.Timechart.TimeSpan;
import org.araqne.logdb.query.command.Timechart.TimeUnit;

public class TimechartParser implements LogQueryCommandParser {
	private static final String COMMAND = "timechart";
	private static final String BY = " by ";

	@Override
	public String getCommandName() {
		return COMMAND;
	}

	@Override
	public LogQueryCommand parse(LogQueryContext context, String commandString) {
		// timechart <options> <aggregation functions> by <clause>
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, COMMAND.length(), Arrays.asList("span"));

		@SuppressWarnings("unchecked")
		Map<String, String> options = (Map<String, String>) r.value;

		String argsPart = commandString.substring(r.next);
		String aggsPart = argsPart;

		List<String> clauses = new ArrayList<String>();

		// parse clauses
		int byPos = QueryTokenizer.findKeyword(argsPart, BY, 0);
		if (byPos > 0) {
			aggsPart = argsPart.substring(0, byPos);
			String clausePart = argsPart.substring(byPos + BY.length());
			clauses = Arrays.asList(clausePart.split(","));
		}

		// parse aggregations
		List<String> aggTerms = QueryTokenizer.parseByComma(aggsPart);
		List<AggregationField> fields = new ArrayList<AggregationField>();

		for (String aggTerm : aggTerms) {
			AggregationField field = AggregationParser.parse(context, aggTerm);
			fields.add(field);
		}

		if (fields.size() == 0)
			throw new LogQueryParseException("need-aggregation-field", COMMAND.length());

		// parse timespan option
		TimeSpan timeSpan = null;
		if (options.containsKey("span"))
			timeSpan = parseTimeSpan(options.get("span"));

		if (timeSpan == null)
			timeSpan = new TimeSpan(1, TimeUnit.Day);

		String clause = null;
		if (!clauses.isEmpty())
			clause = clauses.get(0);

		return new Timechart(fields, clause, timeSpan);
	}

	private static TimeSpan parseTimeSpan(String value) {
		TimeUnit unit = null;
		Integer amount = null;
		int i;
		for (i = 0; i < value.length(); i++) {
			char c = value.charAt(i);
			if (!('0' <= c && c <= '9'))
				break;
		}
		String f = value.substring(i);
		if (f.equalsIgnoreCase("s"))
			unit = TimeUnit.Second;
		else if (f.equalsIgnoreCase("m"))
			unit = TimeUnit.Minute;
		else if (f.equalsIgnoreCase("h"))
			unit = TimeUnit.Hour;
		else if (f.equalsIgnoreCase("d"))
			unit = TimeUnit.Day;
		else if (f.equalsIgnoreCase("w"))
			unit = TimeUnit.Week;
		else if (f.equalsIgnoreCase("mon"))
			unit = TimeUnit.Month;
		else if (f.equalsIgnoreCase("y"))
			unit = TimeUnit.Year;
		amount = Integer.parseInt(value.substring(0, i));

		if (unit == TimeUnit.Month && (amount != 1 && amount != 2 && amount != 3 && amount != 4 && amount != 6))
			throw new LogQueryParseException("invalid-timespan", -1, "month should be 1, 2, 3, 4, or 6");
		if (unit == TimeUnit.Year && amount != 1)
			throw new LogQueryParseException("invalid-timespan", -1, "year should be 1");
		return new TimeSpan(amount, unit);
	}
}
