package org.araqne.logdb.query.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.araqne.logdb.FunctionRegistry;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.query.expr.Comma;
import org.araqne.logdb.query.expr.Expression;
import org.araqne.logdb.query.parser.ExpressionParser.FuncTerm;

public class EvalFuncEmitterFactory implements FuncEmitterFactory {

	private FunctionRegistry functionRegistry;

	public EvalFuncEmitterFactory(FunctionRegistry functionRegistry) {
		this.functionRegistry = functionRegistry;
	}

	@Override
	public void emit(QueryContext context, Stack<Expression> exprStack, FuncTerm f) {
		if (functionRegistry == null)
			throw new IllegalStateException("function registry not set");

		List<Expression> args = getArgsFromStack(f, exprStack);
		Expression func = functionRegistry.newFunction(context, f.getName(), args);
		exprStack.add(func);
	}

	private List<Expression> getArgsFromStack(FuncTerm f, Stack<Expression> exprStack) {
		List<Expression> exprs = null;
		if (exprStack.isEmpty() || !f.hasArgument())
			return new ArrayList<Expression>();

		Expression arg = exprStack.pop();
		if (arg instanceof Comma) {
			exprs = ((Comma) arg).getList();
		} else {
			exprs = new ArrayList<Expression>();
			exprs.add(arg);
		}
		return exprs;
	}
}
