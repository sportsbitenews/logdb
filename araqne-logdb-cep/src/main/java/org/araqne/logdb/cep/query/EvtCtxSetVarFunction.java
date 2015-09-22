package org.araqne.logdb.cep.query;

import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;
import org.araqne.logdb.cep.EventContextService;
import org.araqne.logdb.cep.EventContextStorage;
import org.araqne.logdb.cep.EventKey;
import org.araqne.logdb.query.expr.Expression;

public class EvtCtxSetVarFunction implements Expression {
	private Expression topicExpr;
	private Expression keyExpr;
	private Expression varNameExpr;
	private Expression varDataExpr;
	private Expression hostExpr;

	private EventContextStorage storage;

	public EvtCtxSetVarFunction(QueryContext ctx, List<Expression> exprs, EventContextService eventContextService) {
		this.storage = eventContextService.getDefaultStorage();

		if (exprs.size() != 4 && exprs.size() != 5)
			throw new QueryParseException("invalid-evtctxsetvar-arguments", -1, "argument-count-mismatch");

		this.topicExpr = exprs.get(0);
		this.keyExpr = exprs.get(1);
		this.varNameExpr = exprs.get(2);
		this.varDataExpr = exprs.get(3);

		if (exprs.size() == 5)
			this.hostExpr = exprs.get(4);

	}

	@Override
	public Object eval(Row row) {
		Object arg1 = topicExpr.eval(row);
		Object arg2 = keyExpr.eval(row);

		if (arg1 == null || arg2 == null)
			return false;

		String topic = arg1.toString();
		String key = arg2.toString();

		String host = null;
		if (hostExpr != null) {
			Object arg5 = hostExpr.eval(row);
			if (arg5 != null)
				host = arg5.toString();
		}

		EventKey evtKey = new EventKey(topic, key, host);

		Object arg3 = varNameExpr.eval(row);
		Object arg4 = varDataExpr.eval(row);

		if (arg3 == null)
			return false;

		storage.addContextVariable(evtKey, arg3.toString(), arg4);
		return true;
	}

	@Override
	public String toString() {
		return "evtctxsetvar(" + topicExpr + ", " + keyExpr + ", " + varNameExpr + ", " + varDataExpr + "," + hostExpr + ")";
	}

}
