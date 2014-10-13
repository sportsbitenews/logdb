package org.araqne.logdb.query.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.impl.FunctionRegistryImpl;
import org.araqne.logdb.query.command.Evalc;
import org.araqne.logdb.query.engine.QueryParserServiceImpl;
import org.junit.Before;
import org.junit.Test;

public class EvalcParserTest {
	private QueryParserService queryParserService;
	
	@Before
	public void setup() {
		QueryParserServiceImpl p = new QueryParserServiceImpl();
		p.setFunctionRegistry(new FunctionRegistryImpl());
		queryParserService = p;
	}

	@Test
	public void testError20200() {
		EvalcParser p = new EvalcParser();
		p.setQueryParserService(queryParserService);
		String query = "evalc test";
		Evalc eval = null;
		
		try {
			eval = (Evalc) p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("20200", e.getType());
			assertEquals(6, e.getStartOffset());
			assertEquals(9, e.getEndOffset());	
		}
		assertNull(eval);
	}
	
	@Test
	public void testError20201() {
		EvalcParser p = new EvalcParser();
		p.setQueryParserService(queryParserService);
		String query = "evalc =\"a\"";
		Evalc eval = null;
		
		try {
			eval = (Evalc) p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("20201", e.getType());
			assertEquals(6, e.getStartOffset());
			assertEquals(5, e.getEndOffset());	
		}
		assertNull(eval);
	}
	
	@Test
	public void testError20202() {
		EvalcParser p = new EvalcParser();
		p.setQueryParserService(queryParserService);
		String query = "evalc a=";
		Evalc eval = null;
		
		try {
			eval = (Evalc) p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("20202", e.getType());
			assertEquals(8, e.getStartOffset());
			assertEquals(7, e.getEndOffset());	
		}
		assertNull(eval);
	}
}
