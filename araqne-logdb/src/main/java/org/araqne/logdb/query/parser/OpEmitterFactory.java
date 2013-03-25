package org.araqne.logdb.query.parser;

import java.util.Stack;

import org.araqne.logdb.query.expr.Expression;
import org.araqne.logdb.query.parser.ExpressionParser.OpTerm;

public interface OpEmitterFactory {

	void emit(Stack<Expression> exprStack, OpTerm op);

}
