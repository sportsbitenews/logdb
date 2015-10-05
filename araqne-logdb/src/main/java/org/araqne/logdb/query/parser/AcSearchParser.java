package org.araqne.logdb.query.parser;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.DefaultQuery;
import org.araqne.logdb.Query;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.QueryResultFactory;
import org.araqne.logdb.QueryStatusCallback;
import org.araqne.logdb.TimeSpan;
import org.araqne.logdb.query.command.AcSearch;

public class AcSearchParser extends AbstractQueryCommandParser {
	private QueryParserService parserService;
	private QueryResultFactory resultFactory;

	public AcSearchParser(QueryParserService parserService, QueryResultFactory resultFactory) {
		this.parserService = parserService;
		this.resultFactory = resultFactory;
	}

	@Override
	public String getCommandName() {
		return "acsearch";
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		int b = commandString.indexOf('[');
		int e = commandString.lastIndexOf(']');

		String fieldToken = commandString.substring(getCommandName().length(), b);
		String subQueryString = commandString.substring(b + 1, e).trim();

		List<String> optionTypes = Arrays.asList("timeout");
		ParseResult r = QueryTokenizer.parseOptions(context, fieldToken, 0, optionTypes, getFunctionRegistry());
		@SuppressWarnings("unchecked")
		Map<String, String> options = (Map<String, String>) r.value;

		TimeSpan timeout = null;
		if (options.containsKey("timeout"))
			timeout = TimeSpan.parse(options.get("timeout"));

		String field = fieldToken.substring(r.next).trim();

		QueryContext subQueryContext = new QueryContext(context.getSession(), context);
		List<QueryCommand> subCommands = parserService.parseCommands(subQueryContext, subQueryString);
		Query subQuery = new DefaultQuery(subQueryContext, subQueryString, subCommands, resultFactory);

		SubQueryCountDownLatch subQueryLatch = new SubQueryCountDownLatch();
		subQuery.getCallbacks().getStatusCallbacks().add(subQueryLatch);

		return new AcSearch(field, timeout, subQuery, subQueryLatch.latch, getFunctionRegistry());
	}

	private static class SubQueryCountDownLatch implements QueryStatusCallback {
		private CountDownLatch latch;

		private SubQueryCountDownLatch() {
			this.latch = new CountDownLatch(1);
		}

		@Override
		public void onChange(Query query) {
			if (!query.isFinished())
				return;

			latch.countDown();
		}
	}
}
