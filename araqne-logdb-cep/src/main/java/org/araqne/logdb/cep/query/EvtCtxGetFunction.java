package org.araqne.logdb.cep.query;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;
import org.araqne.logdb.cep.EventContext;
import org.araqne.logdb.cep.EventContextService;
import org.araqne.logdb.cep.EventContextStorage;
import org.araqne.logdb.cep.EventKey;
import org.araqne.logdb.query.expr.Expression;

public class EvtCtxGetFunction implements Expression {
	private Expression topicExpr;
	private Expression keyExpr;
	private Expression fieldExpr;
	private final String field;

	// 1: counter, 2: created, 3: expire_at, 4: host, 5: timeout_at, 6: rows
	private int fieldType;

	private EventContextStorage memStorage;

	// evtctxget("topic", "key", "counter")
	public EvtCtxGetFunction(QueryContext ctx, List<Expression> exprs, EventContextService eventContextService) {
		String engine = System.getProperty("araqne.logdb.cepengine");
		this.memStorage = eventContextService.getStorage(engine);

		if (exprs.size() != 3)
			throw new QueryParseException("invalid-evtctxget-arguments", -1, "argument-count-mismatch");

		this.topicExpr = exprs.get(0);
		this.keyExpr = exprs.get(1);
		this.fieldExpr = exprs.get(2);

		try {
			this.field = fieldExpr.eval(null).toString();
			if (field.equals("counter")) {
				fieldType = 1;
			} else if (field.equals("created")) {
				fieldType = 2;
			} else if (field.equals("expire")) {
				fieldType = 3;
			} else if (field.equals("timeout")) {
				fieldType = 4;
			} else if (field.equals("rows")) {
				fieldType = 5;
			}
		} catch (Throwable t) {
			throw new QueryParseException("invalid-evtctxget-field", -1);
		}

		if (fieldType == 0)
			throw new QueryParseException("unsupported-evtctx-field", -1);
	}

	@Override
	public Object eval(Row row) {
		EventContext ctx = EvtCtxGetFunction.findContext(memStorage, topicExpr, keyExpr, row);
		if (ctx == null)
			return null;

		switch (fieldType) {
		case 1:
			return ctx.getCounter().get();
		case 2:
			return new Date(ctx.getCreated());
		case 3:
			if (ctx.getExpireTime() == 0)
				return null;
			return new Date(ctx.getExpireTime());
		case 4:
			if (ctx.getTimeoutTime() == 0)
				return null;
			return new Date(ctx.getTimeoutTime());
		case 5:
			ArrayList<Object> l = new ArrayList<Object>(ctx.getRows().size());
			for (Row r : ctx.getRows()) {
				l.add(Row.clone(r.map()));
			}

			return l;
		}

		return null;
	}

	@Override
	public String toString() {
		return "evtctxget(" + topicExpr + ", " + keyExpr + ", " + fieldExpr + ")";
	}

	public static EventContext findContext(EventContextStorage storage, Expression topicExpr, Expression keyExpr, Row row) {
		Object arg1 = topicExpr.eval(row);
		Object arg2 = keyExpr.eval(row);

		if (arg1 == null || arg2 == null)
			return null;

		String topic = arg1.toString();
		String key = arg2.toString();

		return storage.getContext(new EventKey(topic, key));
	}
}
