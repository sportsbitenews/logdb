package org.araqne.logdb.query.parser;

import java.util.Stack;

import org.araqne.logdb.query.expr.Expression;
import org.araqne.logdb.query.parser.ExpressionParser.TokenTerm;

public interface TermEmitterFactory {

	void emit(Stack<Expression> exprStack, TokenTerm t);

}
