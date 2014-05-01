package org.araqne.logdb;

import java.util.List;
import java.util.Set;

import org.araqne.logdb.query.expr.Expression;

public interface FunctionRegistry {
	/**
	 * @return all serviced functions including extended functions
	 */
	Set<String> getFunctionNames();
	
	Expression newFunction(QueryContext ctx, String functionName, List<Expression> exprs);

	void registerFactory(FunctionFactory factory);

	void unregisterFactory(FunctionFactory factory);
}
