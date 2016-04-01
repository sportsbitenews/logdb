package org.araqne.rss.command;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.query.parser.CommandOptions;
import org.araqne.logdb.query.parser.ParseResult;
import org.araqne.logdb.query.parser.QueryTokenizer;
import org.araqne.rss.RssReader;

@Component(name = "rss-query-parser")
public class RssCommandParser extends AbstractQueryCommandParser {

	@Requires
	private QueryParserService parserService;

	@Requires
	private RssReader rssReader;

	public RssCommandParser() {
		setOptions("strip", OPTIONAL, "strip html tag", "HTML 태그 제거 여부");
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
	public String getCommandName() {
		return "rss";
	}

	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("23001", new QueryErrorMessage("cannot-read-rss", "RSS에 연결할 수 없습니다."));
		return m;
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(), new ArrayList<String>(),
				getFunctionRegistry());

		Map<String, String> options = (Map<String, String>) r.value;

		boolean stripTag = false;
		if (options.get("strip") != null)
			stripTag = CommandOptions.parseBoolean(options.get("strip"));
		String rssUrl = commandString.substring(r.next).trim();

		return new RssCommand(rssReader, rssUrl, stripTag);
	}
}
