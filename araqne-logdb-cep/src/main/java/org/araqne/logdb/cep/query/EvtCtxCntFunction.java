package org.araqne.logdb.cep.query;

import java.util.Iterator;
import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;
import org.araqne.logdb.cep.EventContextService;
import org.araqne.logdb.cep.EventContextStorage;
import org.araqne.logdb.cep.EventKey;
import org.araqne.logdb.query.expr.Expression;

public class EvtCtxCntFunction implements Expression {
	private EventContextStorage storage;
	private Expression topicExpr;

	public EvtCtxCntFunction(QueryContext ctx, List<Expression> exprs, EventContextService eventContextService) {
		this.storage = eventContextService.getDefaultStorage();

		if (exprs.size() != 1)
			throw new QueryParseException("invalid-evtctxcnt-arguments", -1, "argument-count-mismatch");

		topicExpr = exprs.get(0);
	}

	@Override
	public Object eval(Row row) {
		Object o = topicExpr.eval(row);

		String topic = null;
		if (o != null)
			topic = o.toString();

		Iterator<EventKey> itr = storage.getContextKeys(topic);

		int cnt = 0;
		while (itr.hasNext()) {
			cnt++;
			itr.next();
		}

		return cnt;
	}

	@Override
	public String toString() {
		return "evtctxcnt(" + topicExpr + ")";
	}

}
