package org.araqne.logdb.query.expr;

import org.araqne.logdb.RowBatch;

public interface BatchExpression extends Expression {
	Object[] eval(RowBatch rowBatch);
}
