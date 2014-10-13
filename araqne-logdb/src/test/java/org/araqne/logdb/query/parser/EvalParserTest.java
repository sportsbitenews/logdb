package org.araqne.logdb.query.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.impl.FunctionRegistryImpl;
import org.araqne.logdb.query.command.Eval;
import org.araqne.logdb.query.engine.QueryParserServiceImpl;
import org.junit.Before;
import org.junit.Test;

public class EvalParserTest {
	private QueryParserService queryParserService;
	
	@Before
	public void setup() {
		QueryParserServiceImpl p = new QueryParserServiceImpl();
		p.setFunctionRegistry(new FunctionRegistryImpl());
		queryParserService = p;
	}

	@Test
	public void testConstantExpr() {
		EvalParser p = new EvalParser();
		p.setQueryParserService(queryParserService);

		Eval eval = (Eval) p.parse(null, "eval n=1+2+min(10, 4, 34, -9)");
		Object o = eval.getExpression().eval(null);
		assertEquals(-6L, o);
	}

	@Test
	public void testBrokenEval1() {
		EvalParser p = new EvalParser();
		p.setQueryParserService(queryParserService);
		String query = "eval test";
		
		try {
			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("20100", e.getType());
			assertEquals(5, e.getStartOffset());
			assertEquals(8, e.getEndOffset());	
		}
	}

	@Test
	public void testBrokenEval2() {
		EvalParser p = new EvalParser();
		p.setQueryParserService(queryParserService);
		String query = "eval   =";
		
		try {
			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("20101", e.getType());
			assertEquals(5, e.getStartOffset());
			assertEquals(6, e.getEndOffset());	
		}
		
		query = "eval =";
		
		try {
			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("20101", e.getType());
			assertEquals(5, e.getStartOffset());
			assertEquals(4, e.getEndOffset());	
		}
	}

	@Test
	public void testBrokenEval3() {
		EvalParser p = new EvalParser();
		p.setQueryParserService(queryParserService);
		String query = "eval n=    ";
		
		try {
			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("20102", e.getType());
			assertEquals(7, e.getStartOffset());
			assertEquals(10, e.getEndOffset());	
		}
	}
	
	@Test
	public void testEvalQueryGeneration() {
		EvalParser p = new EvalParser();
		p.setQueryParserService(queryParserService);

		Eval eval = (Eval) p.parse(null, "eval n=1+2");
		assertEquals("eval n=(1 + 2)", eval.toString());
	}
	
	@Test
	public void testError90100() {
		EvalParser p = new EvalParser();
		p.setQueryParserService(queryParserService);
		try {
			p.parse(null, "eval n=abs(-1+ )");
			fail();
		} catch (QueryParseException e) {
			if (e.isDebugMode()) {
				System.out.println(e.getMessage());
			}
			assertEquals("90100", e.getType());
			assertEquals("abs(-1+ )", e.getParams().get("value"));
		}
	}
	
}
