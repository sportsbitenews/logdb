package org.araqne.logdb.query.expr;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;

public class ContextReference implements VectorizedExpression {
	private Map<String, Object> constants;
	private String field;

	public ContextReference(QueryContext context, List<Expression> exprs) {
		this.constants = context.getConstants();
		if (exprs.size() == 0)
			// throw new QueryParseException("null-context-reference", -1);
			throw new QueryParseException("90610", -1, -1, null);
		Object o = exprs.get(0).eval(null);
		if (o == null)
			// throw new QueryParseException("null-context-reference", -1);
			throw new QueryParseException("90611", -1, -1, null);

		this.field = o.toString();
	}

	@Override
	public Object eval(Row map) {
		return constants.get(field);
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		return constants.get(field);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object value = constants.get(field);
		Object[] values = new Object[vbatch.size];
		Arrays.fill(values, value);
		return values;
	}

	@Override
	public String toString() {
		return "$(\"" + field + "\")";
	}
}
