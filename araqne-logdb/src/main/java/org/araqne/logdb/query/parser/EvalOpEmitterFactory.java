package org.araqne.logdb.query.parser;

import java.util.Stack;

import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.expr.*;

public class EvalOpEmitterFactory implements OpEmitterFactory {

	@Override
	public void emit(Stack<Expression> exprStack, Term term) {
		EvalOpTerm op = (EvalOpTerm) term;
		// is unary op?
		if (op.isUnary()) {
			Expression expr = exprStack.pop();
			exprStack.add(new Neg(expr));
			return;
		}

		// reversed order by stack
		if (exprStack.size() < 2)
			throw new QueryParseException("broken-expression", -1, "operator is [" + op + "]");

		Expression rhs = exprStack.pop();
		Expression lhs = exprStack.pop();

		switch (op) {
		case Add:
			exprStack.add(new Add(lhs, rhs));
			break;
		case Sub:
			exprStack.add(new Sub(lhs, rhs));
			break;
		case Mul:
			exprStack.add(new Mul(lhs, rhs));
			break;
		case Div:
			exprStack.add(new Div(lhs, rhs));
			break;
		case Gte:
			exprStack.add(new Gte(lhs, rhs));
			break;
		case Lte:
			exprStack.add(new Lte(lhs, rhs));
			break;
		case Lt:
			exprStack.add(new Lt(lhs, rhs));
			break;
		case Gt:
			exprStack.add(new Gt(lhs, rhs));
			break;
		case And:
			exprStack.add(new And(lhs, rhs));
			break;
		case Or:
			exprStack.add(new Or(lhs, rhs));
			break;
		case Eq:
			exprStack.add(new Eq(lhs, rhs));
			break;
		case Neq:
			exprStack.add(new Neq(lhs, rhs));
			break;
		case Comma:
			exprStack.add(new Comma(lhs, rhs));
			break;
		case ListEndComma:
			exprStack.add(new Comma(lhs, rhs, true));
			break;
		default:
			throw new QueryParseException("unsupported operator", -1, op + " is not supported");
		}
	}
	
}
