package org.araqne.logdb.query.expr;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.araqne.logdb.FunctionRegistry;
import org.araqne.logdb.Row;
import org.araqne.logdb.impl.FunctionRegistryImpl;
import org.araqne.logdb.query.parser.ExpressionParser;
import org.junit.Test;

public class StringReplaceTest {
	@Test
	public void test() {
		assertEquals("cbccb", parseExpr("replace(\"abaab\", \"a\", \"c\")").eval(null));
		assertEquals("abaab", parseExpr("replace(\"abaab\", \"d\", \"c\")").eval(null));
		assertEquals("ab!aab", parseExpr("replace(\"abdaab\", \"d\", \"!\")").eval(null));
	}

	@Test
	public void rexTest() {
		assertEquals("abaab", parseExpr("replace(\"abaab\", \"d\", \"c\", \"re\")").eval(null));
		assertEquals("ab!aab", parseExpr("replace(\"abdaab\", \"d\", \"!\", \"re\")").eval(null));
		assertEquals("cbaab", parseExpr("replace(\"abaab\", \"^a\", \"c\", \"re\")").eval(null));
		assertEquals(
				"2 5 $1 hahaha 12 15 $1",
				parseExpr(
						"replace(\"A:2 B:3 C:5 hahaha A:12 B:13 C:15\", \"A:(\\\\d+) B:\\\\d+ C:(\\\\d+)\", \"$1 $2 \\\\$1\", \"re\")")
						.eval(null));
	}

	@Test
	public void testIssue866() {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", "hello\rworld");
		Row row = new Row(m);

		String text = "replace(line, \"\\r\", \"^^\")";
		StringReplace expr = (StringReplace) parseExpr(text);
		assertEquals("hello^^world", expr.eval(row));
	}

	private FunctionRegistry funcRegistry = new FunctionRegistryImpl();

	private Expression parseExpr(String expr) {
		return ExpressionParser.parse(null, expr, funcRegistry);
	}

}
