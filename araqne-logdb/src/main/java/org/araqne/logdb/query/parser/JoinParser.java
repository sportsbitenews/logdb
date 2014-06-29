package org.araqne.logdb.query.parser;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.DefaultQuery;
import org.araqne.logdb.Query;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryCommandPipe;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.QueryResultFactory;
import org.araqne.logdb.query.command.Join;
import org.araqne.logdb.query.command.Join.JoinType;
import org.araqne.logdb.query.command.Sort;
import org.araqne.logdb.query.command.Sort.SortField;

public class JoinParser extends AbstractQueryCommandParser {

	private QueryParserService parserService;
	private QueryResultFactory resultFactory;

	public JoinParser(QueryParserService parserService, QueryResultFactory resultFactory) {
		this.parserService = parserService;
		this.resultFactory = resultFactory;
	}

	@Override
	public String getCommandName() {
		return "join";
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {

		int b = commandString.indexOf('[');
		int e = commandString.lastIndexOf(']');

		int cmdLen = getCommandName().length();
		String fieldToken = commandString.substring(cmdLen, b);
		String subQueryString = commandString.substring(b + 1, e).trim();

		ParseResult r = QueryTokenizer.parseOptions(context, fieldToken, 0, Arrays.asList("type"), getFunctionRegistry());
		@SuppressWarnings("unchecked")
		Map<String, Object> options = (Map<String, Object>) r.value;

		String type = null;
		if (options != null) {
			type = (String) options.get("type");
		}

		if (r.next < 0)
			r.next = 0;

		JoinType joinType = JoinType.Inner;
		if (type != null && type.equals("left"))
			joinType = JoinType.Left;

		List<SortField> sortFields = SortField.parseSortFields(fieldToken, r);

		// add sort command to end
		SortField[] sortFieldArray = sortFields.toArray(new SortField[0]);
		Sort sort = new Sort(null, sortFieldArray);
		sort.onStart();

		List<QueryCommand> subCommands = parserService.parseCommands(context, subQueryString);
		QueryCommand lastCmd = subCommands.get(subCommands.size() - 1);
		lastCmd.setOutput(new QueryCommandPipe(sort));
		subCommands.add(sort);

		Query subQuery = new DefaultQuery(context, subQueryString, subCommands, resultFactory);
		return new Join(joinType, sortFieldArray, subQuery);
	}
}
