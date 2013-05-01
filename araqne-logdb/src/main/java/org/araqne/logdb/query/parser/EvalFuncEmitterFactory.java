package org.araqne.logdb.query.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.araqne.logdb.LogQueryParseException;
import org.araqne.logdb.query.expr.*;
import org.araqne.logdb.query.parser.ExpressionParser.FuncTerm;

public class EvalFuncEmitterFactory implements FuncEmitterFactory {

	@Override
	public void emit(Stack<Expression> exprStack, FuncTerm f) {
		String name = f.getTokens().remove(0).trim();
		List<Expression> args = getArgsFromStack(exprStack);

		if (name.equals("abs")) {
			exprStack.add(new Abs(args.get(0)));
		} else if (name.equals("min")) {
			exprStack.add(new Min(args));
		} else if (name.equals("case")) {
			exprStack.add(new Case(args));
		} else if (name.equals("if")) {
			exprStack.add(new If(args));
		} else if (name.equals("concat")) {
			exprStack.add(new Concat(args));
		} else if (name.equals("str")) {
			exprStack.add(new ToString(args));
		} else if (name.equals("long")) {
			exprStack.add(new ToLong(args));
		} else if (name.equals("int")) {
			exprStack.add(new ToInt(args));
		} else if (name.equals("double")) {
			exprStack.add(new ToDouble(args));
		} else if (name.equals("date")) {
			exprStack.add(new ToDate(args));
		} else if (name.equals("string")) {
			exprStack.add(new ToString(args));
		} else if (name.equals("left")) {
			exprStack.add(new Left(args));
		} else if (name.equals("right")) {
			exprStack.add(new Right(args));
		} else if (name.equals("trim")) {
			exprStack.add(new Trim(args));
		} else if (name.equals("len")) {
			exprStack.add(new Len(args));
		} else if (name.equals("substr")) {
			exprStack.add(new Substr(args));
		} else if (name.equals("isnull")) {
			exprStack.add(new IsNull(args));
		} else if (name.equals("isnotnull")) {
			exprStack.add(new IsNotNull(args));
		} else if (name.equals("isnum")) {
			exprStack.add(new IsNum(args));
		} else if (name.equals("isstr")) {
			exprStack.add(new IsStr(args));
		} else if (name.equals("match")) {
			exprStack.add(new Match(args));
		} else if (name.equals("typeof")) {
			exprStack.add(new Typeof(args));
		} else if (name.equals("in")) {
			exprStack.add(new In(args));
		} else {
			throw new LogQueryParseException("unsupported-function", -1, "function name is " + name);
		}
	}

	private List<Expression> getArgsFromStack(Stack<Expression> exprStack) {
		List<Expression> exprs = null; 
		Expression arg = exprStack.pop();
		if (arg instanceof Comma) {
			exprs = ((Comma)arg).getList();
		}
		else {
			exprs = new ArrayList<Expression>();
			exprs.add(arg);
		}
		// TODO : If it is Comma expression, get argument list from it
		return exprs;
	}

	private static List<Expression> parseArgs(List<String> tokens) {
		// separate by outermost comma (not in nested function call)
		List<Expression> exprs = new ArrayList<Expression>();

		int parensCount = 0;

		List<String> subTokens = new ArrayList<String>();
		tokens = tokens.subList(1, tokens.size() - 1);

		for (String token : tokens) {
			String t = token.trim();
			if (t.equals("("))
				parensCount++;
			if (t.equals(")"))
				parensCount--;

			if (parensCount == 0 && t.equals(",")) {
				exprs.add(parseArg(subTokens));
				subTokens = new ArrayList<String>();
			} else
				subTokens.add(token);
		}

		exprs.add(parseArg(subTokens));
		return exprs;
	}

	private static Expression parseArg(List<String> tokens) {
		StringBuilder sb = new StringBuilder();
		for (String token : tokens) {
			sb.append(token);
		}

		return ExpressionParser.parse(sb.toString());
	}



}
