package org.araqne.logdb.query.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.araqne.logdb.LogQueryContext;
import org.araqne.logdb.LogQueryParseException;
import org.araqne.logdb.query.expr.*;
import org.araqne.logdb.query.parser.ExpressionParser.FuncTerm;

public class EvalFuncEmitterFactory implements FuncEmitterFactory {

	@Override
	public void emit(LogQueryContext context, Stack<Expression> exprStack, FuncTerm f) {
		String name = f.getName();
		List<Expression> args = getArgsFromStack(f, exprStack);

		if (name.equals("abs")) {
			exprStack.add(new Abs(args.get(0)));
		} else if (name.equals("max")) {
			exprStack.add(new Max(args));
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
		} else if (name.equals("epoch")) {
			exprStack.add(new Epoch(args));
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
		} else if (name.equals("ip")) {
			exprStack.add(new ToIp(args));
		} else if (name.equals("network")) {
			exprStack.add(new Network(args));
		} else if (name.equals("urldecode")) {
			exprStack.add(new UrlDecode(args));
		} else if (name.equals("lower")) {
			exprStack.add(new Lower(args));
		} else if (name.equals("upper")) {
			exprStack.add(new Upper(args));
		} else if (name.equals("dateadd")) {
			exprStack.add(new DateAdd(args));
		} else if (name.equals("now")) {
			exprStack.add(new Now(args));
		} else if (name.equals("datediff")) {
			exprStack.add(new DateDiff(args));
		} else if (name.equals("$")) {
			exprStack.add(new ContextReference(context, args));
		} else if (name.equals("guid")) {
			exprStack.add(new Guid());
		} else if (name.equals("seq")) {
			exprStack.add(new Seq());
		} else if (name.equals("ip2long")) {
			exprStack.add(new Ip2Long(args));
		} else if (name.equals("long2ip")) {
			exprStack.add(new Long2Ip(args));
		} else if (name.equals("round")) {
			exprStack.add(new Round(args));
		} else if (name.equals("floor")) {
			exprStack.add(new Floor(args));
		} else if (name.equals("ceil")) {
			exprStack.add(new Ceil(args));
		} else {
			throw new LogQueryParseException("unsupported-function", -1, "function name is " + name);
		}
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
