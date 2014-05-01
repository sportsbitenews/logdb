package org.araqne.logdb;

import java.util.List;
import java.util.Set;

import org.araqne.logdb.query.expr.Expression;

public interface FunctionFactory {
	/**
	 * @return supported function names
	 */
	Set<String> getFunctionNames();

	Expression newFunction(QueryContext ctx, String name, List<Expression> exprs);
}
