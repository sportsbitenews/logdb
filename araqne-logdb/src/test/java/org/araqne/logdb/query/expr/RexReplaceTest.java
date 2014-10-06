package org.araqne.logdb.query.expr;

import static org.junit.Assert.assertEquals;

import org.araqne.logdb.FunctionRegistry;
import org.araqne.logdb.impl.FunctionRegistryImpl;
import org.araqne.logdb.query.parser.ExpressionParser;
import org.junit.Test;

public class RexReplaceTest {
	@Test
	public void test() {
		assertEquals("abaab", parseExpr("rexreplace(\"abaab\", \"d\", \"c\")").eval(null));
		assertEquals("ab!aab", parseExpr("rexreplace(\"abdaab\", \"d\", \"!\")").eval(null));
		assertEquals("cbaab", parseExpr("rexreplace(\"abaab\", \"^a\", \"c\")").eval(null));
		assertEquals("2 5 $1 hahaha 12 15 $1", 
				parseExpr("rexreplace(\"A:2 B:3 C:5 hahaha A:12 B:13 C:15\", \"A:(\\\\d+) B:\\\\d+ C:(\\\\d+)\", \"$1 $2 \\\\$1\")").eval(null));
	}

	private FunctionRegistry funcRegistry = new FunctionRegistryImpl();

	private Expression parseExpr(String expr) {
		return ExpressionParser.parse(null, expr, funcRegistry);
	}
}
