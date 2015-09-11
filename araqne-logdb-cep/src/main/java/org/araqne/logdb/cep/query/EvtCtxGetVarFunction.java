package org.araqne.logdb.cep.query;

import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;
import org.araqne.logdb.cep.EventContext;
import org.araqne.logdb.cep.EventContextService;
import org.araqne.logdb.cep.EventContextStorage;
import org.araqne.logdb.query.expr.Expression;

public class EvtCtxGetVarFunction implements Expression {
	private Expression topicExpr;
	private Expression keyExpr;
	private Expression varNameExpr;
	private Expression hostExpr;

	private EventContextStorage storage;

	public EvtCtxGetVarFunction(QueryContext ctx, List<Expression> exprs, EventContextService eventContextService) {
		this.storage = eventContextService.getDefaultStorage();

		if (exprs.size() != 3 && exprs.size() != 4)
			throw new QueryParseException("invalid-evtctxgetvar-arguments", -1, "argument-count-mismatch");

		this.topicExpr = exprs.get(0);
		this.keyExpr = exprs.get(1);
		this.varNameExpr = exprs.get(2);

		if (exprs.size() == 4)
			this.hostExpr = exprs.get(3);

	}

	@Override
	public Object eval(Row row) {
		EventContext ctx = EvtCtxGetFunction.findContext(storage, topicExpr, keyExpr, hostExpr, row);
		if (ctx == null)
			return null;

		Object arg3 = varNameExpr.eval(row);
		if (arg3 == null)
			return null;

		return ctx.getVariable(arg3.toString());
	}

	@Override
	public String toString() {
		return "evtctxgetvar(" + topicExpr + ", " + keyExpr + ", " + varNameExpr + ", " + hostExpr + ")";
	}

}
