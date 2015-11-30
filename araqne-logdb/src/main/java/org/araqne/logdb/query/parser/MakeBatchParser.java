package org.araqne.logdb.query.parser;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.query.command.MakeBatch;

public class MakeBatchParser extends AbstractQueryCommandParser {

	public MakeBatchParser() {
		setDescriptions("Make a batch of rows from single rows be input for every 0.1 second.",
				"개별로 들어오는 입력을 0.1 초 간격으로 모아 배치로 출력합니다.");
	}

	@Override
	public String getCommandName() {
		return "makebatch";
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		return new MakeBatch();
	}

}
