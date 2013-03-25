package org.araqne.logdb.query.parser;

import java.util.Stack;

import org.araqne.logdb.query.expr.Expression;
import org.araqne.logdb.query.parser.ExpressionParser.FuncTerm;

public interface FuncEmitterFactory {

	void emit(Stack<Expression> exprStack, FuncTerm f);

}
