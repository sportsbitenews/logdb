package org.araqne.logdb.query.parser;

import org.araqne.logdb.LogQueryCommand;
import org.araqne.logdb.LogQueryCommandParser;
import org.araqne.logdb.LogQueryContext;
import org.araqne.logdb.LogQueryParseException;
import org.araqne.logdb.query.command.Eval;
import org.araqne.logdb.query.command.Set;
import org.araqne.logdb.query.expr.Expression;

public class SetParser implements LogQueryCommandParser {

	@Override
	public String getCommandName() {
		return "set";
	}

	@Override
	public LogQueryCommand parse(LogQueryContext context, String commandString) {
		// find assignment symbol
		int p = QueryTokenizer.findKeyword(commandString, "=");
		if (p < 0)
			throw new LogQueryParseException("assign-token-not-found", commandString.length());

		String field = commandString.substring(getCommandName().length(), p).trim();
		String exprToken = commandString.substring(p + 1).trim();

		if (field.isEmpty())
			throw new LogQueryParseException("field-name-not-found", commandString.length());

		if (exprToken.isEmpty())
			throw new LogQueryParseException("expression-not-found", commandString.length());

		Expression expr = ExpressionParser.parse(context, exprToken);
		context.getConstants().put(field, expr.eval(null));
		return new Set(field, expr);
	}

}
