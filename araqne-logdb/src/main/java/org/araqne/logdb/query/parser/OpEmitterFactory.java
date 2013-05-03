package org.araqne.logdb.query.parser;

import java.util.Stack;

import org.araqne.logdb.query.expr.Expression;

public interface OpEmitterFactory {

	void emit(Stack<Expression> exprStack, Term term);
	
}
