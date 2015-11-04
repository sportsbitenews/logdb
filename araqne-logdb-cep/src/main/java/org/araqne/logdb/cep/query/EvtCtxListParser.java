package org.araqne.logdb.cep.query;

import java.util.Arrays;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.cep.EventContextService;
import org.araqne.logdb.query.parser.ParseResult;
import org.araqne.logdb.query.parser.QueryTokenizer;

@Component(name = "evtctxlist-parser")
public class EvtCtxListParser extends AbstractQueryCommandParser {
	@Requires
	private QueryParserService queryParserService;

	@Requires
	private EventContextService eventContextService;

	public EvtCtxListParser() {
		setDescriptions("Scan all event contexts.", "이벤트 컨텍스트 목록을 조회합니다.");
		setOptions("topic", false, "Filter by topic.", "이벤트 컨텍스트 토픽으로 필터링합니다.");
	}

	@Override
	public String getCommandName() {
		return "evtctxlist";
	}

	@Validate
	public void start() {
		queryParserService.addCommandParser(this);
	}

	@Invalidate
	public void stop() {
		if (queryParserService != null)
			queryParserService.removeCommandParser(this);
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(), Arrays.asList("topic"),
				getFunctionRegistry());

		Map<String, String> options = (Map<String, String>) r.value;
		String topicFilter = options.get("topic");

		return new EvtCtxListCommand(eventContextService, topicFilter);
	}
}
