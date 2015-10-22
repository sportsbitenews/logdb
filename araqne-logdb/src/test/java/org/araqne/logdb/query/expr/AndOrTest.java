package org.araqne.logdb.query.expr;

import static org.junit.Assert.*;

import java.util.HashMap;

import org.araqne.logdb.Row;
import org.junit.Test;

public class AndOrTest {
	@Test
	public void simpleAndTest() {
		Expression expr = FunctionUtil.parseExpr("a==1 and b");
		HashMap<String, Object> row = new HashMap<String, Object>();
		assertFalse(Boolean.class.cast(expr.eval(new Row(row))));

		row.put("b", 1);
		assertFalse(Boolean.class.cast(expr.eval(new Row(row))));

		row.put("a", 1);
		assertTrue(Boolean.class.cast(expr.eval(new Row(row))));

		row.put("a", 2);
		assertFalse(Boolean.class.cast(expr.eval(new Row(row))));
	}

	@Test
	public void simpleOrTest() {
		Expression expr = FunctionUtil.parseExpr("a==1 or b");
		HashMap<String, Object> row = new HashMap<String, Object>();
		assertFalse(Boolean.class.cast(expr.eval(new Row(row))));

		row.put("b", 1);
		assertTrue(Boolean.class.cast(expr.eval(new Row(row))));

		row.put("a", 1);
		assertTrue(Boolean.class.cast(expr.eval(new Row(row))));

		row.put("a", 2);
		assertTrue(Boolean.class.cast(expr.eval(new Row(row))));
		
		row.remove("b");
		assertFalse(Boolean.class.cast(expr.eval(new Row(row))));
	}
	
	@Test
	public void longExpressionTest() {
		{
			Expression expr = FunctionUtil.parseExpr("true or true or true or true");
			assertTrue(Boolean.class.cast(expr.eval(null)));
		}
		
		{
			Expression expr = FunctionUtil.parseExpr("true or false or true or true");
			assertTrue(Boolean.class.cast(expr.eval(null)));
		}
		
		{
			Expression expr = FunctionUtil.parseExpr("false or false or false or true");
			assertTrue(Boolean.class.cast(expr.eval(null)));
		}
		
		{
			Expression expr = FunctionUtil.parseExpr("false or false or false or false");
			assertFalse(Boolean.class.cast(expr.eval(null)));
		}
		
		{
			Expression expr = FunctionUtil.parseExpr("true and false and true and true");
			assertFalse(Boolean.class.cast(expr.eval(null)));
		}
		
		{
			Expression expr = FunctionUtil.parseExpr("false and false and false and true");
			assertFalse(Boolean.class.cast(expr.eval(null)));
		}
		
		{
			Expression expr = FunctionUtil.parseExpr("false and false and false and false");
			assertFalse(Boolean.class.cast(expr.eval(null)));
		}

		{
			Expression expr = FunctionUtil.parseExpr("true and true and true and true");
			assertTrue(Boolean.class.cast(expr.eval(null)));
		}
	}

}
