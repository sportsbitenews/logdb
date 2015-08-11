package org.araqne.logdb.query.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.Row;
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

		{
			Eval eval = (Eval) p.parse(null, "eval n=1+2+min(10, 4, 34, -9)");
			Row r = new Row();
			eval.onPush(r);
			assertEquals(1, r.map().size());
			assertEquals(-6L, r.get("n"));
		}

		{
			Eval eval = (Eval) p.parse(null, "eval n=1+2+min(10, 4, 34, -9), t=\"asdf\"");
			Row r = new Row();
			eval.onPush(r);
			assertEquals(2, r.map().size());
			assertEquals(-6L, r.get("n"));
			assertEquals("asdf", r.get("t"));
		}

		{
			Eval eval = (Eval) p.parse(null, "eval b=1==2");
			Row r = new Row();
			eval.onPush(r);
			assertEquals(1, r.map().size());
			assertEquals(false, r.get("b"));
		}

		{
			Eval eval = (Eval) p.parse(null, "eval a=b=1+2-5, c=2, d=3");
			Row r = new Row();
			eval.onPush(r);
			assertEquals(4, r.map().size());
			assertEquals(-2L, r.get("a"));
			assertEquals(-2L, r.get("b"));
			assertEquals(2, r.get("c"));
			assertEquals(3, r.get("d"));
		}
		
		{
			Eval eval = (Eval) p.parse(null, "eval (seq=min(1, 3, 4, -1))");
			Row r = new Row();
			eval.onPush(r);
			assertEquals(1, r.map().size());
			assertEquals(-1, r.get("seq"));
		}

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
			assertEquals("90100", e.getType());
			assertEquals(5, e.getStartOffset());
			assertEquals(7, e.getEndOffset());	
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
			assertEquals("90100", e.getType());
			assertEquals(5, e.getStartOffset());
			assertEquals(5, e.getEndOffset());	
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
			assertEquals("90100", e.getType());
			assertEquals(5, e.getStartOffset());
			assertEquals(10, e.getEndOffset());	
		}
	}
	
	@Test
	public void testEvalQueryGeneration() {
		EvalParser p = new EvalParser();
		p.setQueryParserService(queryParserService);

		Eval eval = (Eval) p.parse(null, "eval n=1+2");
		assertEquals("eval (n = (1 + 2))", eval.toString());
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
			assertEquals("n=abs(-1+ )", e.getParams().get("value"));
		}
	}
	
}
