package org.araqne.logdb.query.expr;

import static org.junit.Assert.assertEquals;

import org.araqne.logdb.FunctionRegistry;
import org.araqne.logdb.impl.FunctionRegistryImpl;
import org.araqne.logdb.query.parser.ExpressionParser;
import org.junit.Test;

public class StringReplaceTest {
	@Test
	public void test() {
		assertEquals("cbccb", parseExpr("strreplace(\"abaab\", \"a\", \"c\")").eval(null));
		assertEquals("abaab", parseExpr("strreplace(\"abaab\", \"d\", \"c\")").eval(null));
		assertEquals("ab!aab", parseExpr("strreplace(\"abdaab\", \"d\", \"!\")").eval(null));
	}

	private FunctionRegistry funcRegistry = new FunctionRegistryImpl();

	private Expression parseExpr(String expr) {
		return ExpressionParser.parse(null, expr, funcRegistry);
	}

}
