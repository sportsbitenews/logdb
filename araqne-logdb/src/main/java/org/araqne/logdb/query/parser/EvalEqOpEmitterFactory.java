/*
 * Copyright 2015 Eediom Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.araqne.logdb.query.parser;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.expr.*;

public class EvalEqOpEmitterFactory implements OpEmitterFactory {
	@Override
	public void emit(Stack<Expression> exprStack, Term term) {
		EvalEqOpTerm op = (EvalEqOpTerm) term;
		// is unary op?
		if (op.isUnary()) {
			Expression expr = exprStack.pop();
			exprStack.add(new Neg(expr));
			return;
		}

		// reversed order by stack
		if (exprStack.size() < 2){
//			throw new QueryParseException("broken-expression", -1, "operator is [" + op + "]");
			Map<String, String> params = new HashMap<String, String > ();
			params.put("option", op.toString());
			throw new QueryParseException("90100", -1, -1, params);
}

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
		case Assign:
			exprStack.add(new Assign(lhs, rhs));
			break;
		default:
		//	throw new QueryParseException("unsupported operator", -1, op + " is not supported");
			Map<String, String> params = new HashMap<String, String > ();
			params.put("op", op.toString());
			throw new QueryParseException("90101", -1, -1, params);
		}
	}
}
