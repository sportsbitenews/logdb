package org.araqne.logdb.hprof;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParserService;

@Component(name = "jmap-command-parser")
public class JmapCommandParser extends AbstractQueryCommandParser {

	@Requires
	private QueryParserService parserService;

	public JmapCommandParser() {
		setDescriptions("Load heap dump information in hprof binary file.", "hprof 바이너리 포맷으로 된 파일의 힙 덤프 정보를 조회합니다.");
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
		return "jmap";
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		return new JmapCommand(commandString.substring(getCommandName().length() + 1));
	}

}