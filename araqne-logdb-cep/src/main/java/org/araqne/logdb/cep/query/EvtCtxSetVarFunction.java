package org.araqne.logdb.cep.query;

import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;
import org.araqne.logdb.cep.EventContext;
import org.araqne.logdb.cep.EventContextService;
import org.araqne.logdb.cep.EventContextStorage;
import org.araqne.logdb.query.expr.Expression;

public class EvtCtxSetVarFunction implements Expression {
	private Expression topicExpr;
	private Expression keyExpr;
	private Expression varNameExpr;
	private Expression varDataExpr;

	private EventContextStorage storage;

	public EvtCtxSetVarFunction(QueryContext ctx, List<Expression> exprs, EventContextService eventContextService) {
		String engine = System.getProperty("araqne.logdb.cepengine");
		this.storage = eventContextService.getStorage(engine);

		if (exprs.size() != 4)
			throw new QueryParseException("invalid-evtctxsetvar-arguments", -1, "argument-count-mismatch");

		this.topicExpr = exprs.get(0);
		this.keyExpr = exprs.get(1);
		this.varNameExpr = exprs.get(2);
		this.varDataExpr = exprs.get(3);
	}

	@Override
	public Object eval(Row row) {
		EventContext ctx = EvtCtxGetFunction.findContext(storage, topicExpr, keyExpr, row);
		if (ctx == null)
			return false;

		Object arg3 = varNameExpr.eval(row);
		if (arg3 == null)
			return false;

		Object arg4 = varDataExpr.eval(row);

		ctx.setVariable(arg3.toString(), arg4);
		
		storage.addContext(ctx);
		
		return true;
	}

	@Override
	public String toString() {
		return "evtctxsetvar(" + topicExpr + ", " + keyExpr + ", " + varNameExpr + ", " + varDataExpr + ")";
	}

}
