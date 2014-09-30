/*
 * Copyright 2013 Future Systems
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.araqne.logdb.FunctionRegistry;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseInsideException;
import org.araqne.logdb.Row;
import org.araqne.logdb.impl.FunctionRegistryImpl;
import org.araqne.logdb.query.expr.Expression;
import org.araqne.logdb.query.expr.StringConstant;
import org.junit.Test;

public class ExpressionParserTest {
	@Test
	public void testSimple() {
		Expression expr = parseExpr("3+4*2/(1-5)");
		Object v = expr.eval(null);
		assertEquals(1.0, v);
	}

	private FunctionRegistry funcRegistry = new FunctionRegistryImpl();

	private Expression parseExpr(String expr) {
		return ExpressionParser.parse(null, expr, funcRegistry);
	}

	@Test
	public void testFuncExpr() {
		Expression expr = parseExpr("1 + abs(1-5*2)");
		Object v = expr.eval(null);
		assertEquals(10L, v);
	}

	@Test
	public void testFuncMultiArgs() {
		Row log = new Row();
		log.put("test", 1);

		Expression expr = parseExpr("100 + min(3, 7, 2, 5, test) * 2");
		Object v = expr.eval(log);
		assertEquals(102L, v);
	}

	@Test
	public void testNestedFuncExpr() {
		Expression expr = parseExpr("min(abs(1-9), 3, 10, 5)");
		Object v = expr.eval(null);
		assertEquals(3, v);

		expr = parseExpr("concat(\"this\", \" \", concat(\"is\", concat(\" \", \"a\", \" \", \"cat\")), \".\")");
		v = expr.eval(null);
		assertEquals("this is a cat.", v);
	}

	@Test
	public void testCommaExpr() {
		Expression expr = parseExpr("1, 2");
		Object v = expr.eval(null);
		assertEquals("[1, 2]", v.toString());

		expr = parseExpr("1, 2, (3, 4), 5");
		v = expr.eval(null);
		assertEquals("[1, 2, [3, 4], 5]", v.toString());

		expr = parseExpr("(1, 2), (3, 4), 5");
		v = expr.eval(null);
		assertEquals("[[1, 2], [3, 4], 5]", v.toString());

		expr = parseExpr("(1, 2, 3, (4, 5, 6), 7), 5");
		v = expr.eval(null);
		assertEquals("[[1, 2, 3, [4, 5, 6], 7], 5]", v.toString());
	}

	@Test
	public void testNegation() {
		Expression expr = parseExpr("-abs(1-9) * 2");
		Object v = expr.eval(null);
		assertEquals(-16L, v);

		expr = parseExpr("--2");
		Number n = (Number) expr.eval(null);
		assertEquals(2L, n.longValue());

		expr = parseExpr("1+-2");
		v = expr.eval(null);
		assertEquals(-1L, v);

		expr = parseExpr("3--5");
		v = expr.eval(null);
		assertEquals(8L, v);

		expr = parseExpr("3*-5");
		v = expr.eval(null);
		assertEquals(-15L, v);
	}

	@Test
	public void testBrokenExpr() {
		String invalid = "3+4*2/";
		try {
			parseExpr(invalid);
			fail();
		} catch (QueryParseInsideException e) {
			if(e.isDebugMode()){
				System.out.println("query " + invalid);
				System.out.println(e.getMessage());
			}
			assertEquals("90100", e.getType());
			assertEquals(invalid, e.getParams().get("value"));
		} 

		invalid = "3 4*2";
		try {
			parseExpr(invalid);
			fail();
		} catch (QueryParseInsideException e) {
			if(e.isDebugMode()){
				System.out.println("query " + invalid);
				System.out.println(e.getMessage());
			}
			assertEquals("90201", e.getType());
			assertEquals(invalid, e.getParams().get("value"));
		}
	}

	@Test
	public void testGreaterThanEqual() {
		Expression exp = parseExpr("10 >= 3");
		assertTrue((Boolean) exp.eval(null));

		exp = parseExpr("3 >= 3");
		assertTrue((Boolean) exp.eval(null));
	}

	@Test
	public void testGreaterThan() {
		Expression exp = parseExpr("10 > 3");
		assertTrue((Boolean) exp.eval(null));

		exp = parseExpr("3 > 3");
		assertFalse((Boolean) exp.eval(null));
	}

	@Test
	public void testLesserThanEqual() {
		Expression exp = parseExpr("3 <= 5");
		assertTrue((Boolean) exp.eval(null));

		exp = parseExpr("3 <= 3");
		assertTrue((Boolean) exp.eval(null));
	}

	@Test
	public void testLesserThan() {
		Expression exp = parseExpr("3 < 5");
		assertTrue((Boolean) exp.eval(null));

		exp = parseExpr("3 < 3");
		assertFalse((Boolean) exp.eval(null));
	}

	@Test
	public void testBooleanArithmeticPrecendence() {
		Expression exp = parseExpr("1 == 3-2 or 2 == 2");
		assertTrue((Boolean) exp.eval(null));
	}

	@Test
	public void testEq() {
		Expression exp = parseExpr("1 == 0");
		assertFalse((Boolean) exp.eval(null));
	}

	@Test
	public void testAnd() {
		Expression exp = parseExpr("10 >= 3 and 1 == 0");
		assertFalse((Boolean) exp.eval(null));
	}

	@Test
	public void testAndOr() {
		Expression exp = parseExpr("10 >= 3 and (1 == 0 or 2 == 2)");
		assertTrue((Boolean) exp.eval(null));
	}

	@Test
	public void testIf() {
		Expression exp = parseExpr("if(field >= 10, 10, field)");

		Row m1 = new Row();
		m1.put("field", 15);
		assertEquals(10, exp.eval(m1));

		Row m2 = new Row();
		m2.put("field", 3);
		assertEquals(3, exp.eval(m2));
	}

	@Test
	public void testCase() {
		Expression exp = parseExpr("case(field >= 10, 10, field < 10, field)");

		Row m1 = new Row();
		m1.put("field", 15);
		assertEquals(10, exp.eval(m1));

		Row m2 = new Row();
		m2.put("field", 3);
		assertEquals(3, exp.eval(m2));
	}

	@Test
	public void testConcat() {
		Expression exp = parseExpr("concat(\"hello\", \"world\")");
		assertEquals("helloworld", exp.eval(null));
	}

	@Test
	public void testToDate() {
		Calendar c = Calendar.getInstance();
		c.set(Calendar.YEAR, 2013);
		c.set(Calendar.MONTH, 1);
		c.set(Calendar.DAY_OF_MONTH, 6);
		c.set(Calendar.HOUR_OF_DAY, 11);
		c.set(Calendar.MINUTE, 26);
		c.set(Calendar.SECOND, 33);
		c.set(Calendar.MILLISECOND, 0);

		Row m = new Row();
		m.put("date", "2013-02-06 11:26:33");

		Expression exp = parseExpr("date(date, \"yyyy-MM-dd HH:mm:ss\")");
		Object v = exp.eval(m);
		assertEquals(c.getTime(), v);
	}

	@Test
	public void testSubstr() {
		String s = "abcdefg";

		Row m = new Row();
		m.put("line", s);

		Expression exp = parseExpr("substr(line,0,7)");
		assertEquals("abcdefg", exp.eval(m));

		exp = parseExpr("substr(line,0,0)");
		assertEquals("", exp.eval(m));

		exp = parseExpr("substr(line,8,10)");
		assertNull(exp.eval(m));

		exp = parseExpr("substr(line,3,6)");
		assertEquals("def", exp.eval(m));
	}

	@Test
	public void testMatch() {
		String s = "210.119.122.32";
		Row m = new Row();
		m.put("line", s);

		Expression exp = parseExpr("match(line, \"210.*\")");
		assertTrue((Boolean) exp.eval(m));

		exp = parseExpr("match(line, \"192.*\")");
		assertFalse((Boolean) exp.eval(m));
	}

	@Test
	public void testWildcard() {
		Expression exp = parseExpr("\"210.119.122.32\" == \"210*\"");
		assertTrue((Boolean) exp.eval(null));

		exp = parseExpr("\"210.119.122.32\" == \"*32\"");
		assertTrue((Boolean) exp.eval(null));

		exp = parseExpr("\"210.119.122.32\" == \"119*\"");
		assertFalse((Boolean) exp.eval(null));

		exp = parseExpr("\"210.119.122.32\" == \"119\"");
		assertFalse((Boolean) exp.eval(null));
	}

	@Test
	public void testBooleanConstants() {
		Expression exp1 = parseExpr("field == true");
		Row m = new Row();
		m.put("field", true);
		assertTrue((Boolean) exp1.eval(m));

		m = new Row();
		m.put("field", false);
		assertFalse((Boolean) exp1.eval(m));

		Expression exp2 = parseExpr("field == false");
		m = new Row();
		m.put("field", false);
		assertTrue((Boolean) exp2.eval(m));
	}

	@Test
	public void testInIntegers() {
		Expression expr = parseExpr("in(field, 1, 2, 3)");

		Row m = new Row();
		m.put("field", 1);
		assertTrue((Boolean) expr.eval(m));

		m.put("field", 2);
		assertTrue((Boolean) expr.eval(m));

		m.put("field", 3);
		assertTrue((Boolean) expr.eval(m));

		m.put("field", 4);
		assertFalse((Boolean) expr.eval(m));

		m.put("field", null);
		assertFalse((Boolean) expr.eval(m));
	}

	@Test
	public void testInStrings() {
		Expression expr = parseExpr("in(field, \"a\", \"b\", \"c\")");

		Row m = new Row();
		m.put("field", "a");
		assertTrue((Boolean) expr.eval(m));

		m.put("field", "b");
		assertTrue((Boolean) expr.eval(m));

		m.put("field", "c");
		assertTrue((Boolean) expr.eval(m));

		m.put("field", "d");
		assertFalse((Boolean) expr.eval(m));
	}

	@Test
	public void testInStringWildcards() {
		Expression expr = parseExpr("in(field, \"*74.86.*\")");

		Row m = new Row();
		m.put("field", "ip = 74.86.1.2");
		assertTrue((Boolean) expr.eval(m));

		m.put("field", "ip = 75.81.1.2");
		assertFalse((Boolean) expr.eval(m));
	}

	@Test
	public void testBracket() {
		{
			Expression expr = parseExpr("a == \"*[GameStart REP]*\"");
			Row m = new Row();
			m.put("a",
					"22:27:05.235(tid=4436)[Q=0:1:0:0]I[10.1.119.86-997014784-8439] [0 ms][GameStart REP]=126:200:3111 0073875:61.111.10.21:59930:2:1:0:101:qa161새롱 1:2:2718376:3:2000015:0");
			assertTrue((Boolean) expr.eval(m));
		}
		{
			Expression expr = parseExpr("a == \"*[GameStart REP]*\"");
			Row m = new Row();
			m.put("a",
					"22:27:05.235(tid=4436)[Q=0:1:0:0]I[10.1.119.86-997014784-8439] [0 ms][GameStrt REP]=126:200:3111 0073875:61.111.10.21:59930:2:1:0:101:qa161새롱 1:2:2718376:3:2000015:0");
			assertFalse((Boolean) expr.eval(m));
		}
	}

	@Test
	public void testStringEscape() {
		String newline = "\"hello\\nworld\"";
		StringConstant expr = (StringConstant) parseExpr(newline);
		assertEquals("hello\nworld", expr.getConstant());

		String tab = "\"hello\\tworld\\\\\"";
		StringConstant expr2 = (StringConstant) parseExpr(tab);
		assertEquals("hello\tworld\\", expr2.getConstant());

		String invalid = "\"hello\\tworld\\i\"";
		try {
			parseExpr(invalid);
			fail();
		
		} catch (QueryParseInsideException e) {
			if(e.isDebugMode()){
				System.out.println(invalid);
				System.out.println(e.getMessage());
			}
			assertEquals("90205", e.getType());
			assertEquals("\\i", e.getParams().get("escape"));
			assertEquals(invalid, e.getParams().get("value"));
		}
	}

	@Test
	public void testFuncNoArg() {
		QueryContext context = new QueryContext(null);
		Expression expr = ExpressionParser.parse(context, "string(now(), \"yyyyMMdd\")", funcRegistry);
		assertEquals(expr.eval(null), new SimpleDateFormat("yyyyMMdd").format(new Date()));

		expr = ExpressionParser.parse(context, "concat(\"a\",string(now(), \"yyyyMMdd\"))", funcRegistry);
		assertEquals(expr.eval(null), "a" + new SimpleDateFormat("yyyyMMdd").format(new Date()));
	}
}
