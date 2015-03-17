package org.araqne.logdb.query.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.impl.FunctionRegistryImpl;
import org.araqne.logdb.query.engine.QueryParserServiceImpl;
import org.junit.Before;
import org.junit.Test;

public class ExplodeParserTest {
	private QueryParserService queryParserService;

	@Before
	public void setup() {
		QueryParserServiceImpl p = new QueryParserServiceImpl();
		p.setFunctionRegistry(new FunctionRegistryImpl());
		queryParserService = p;
	}

	@Test
	public void testError20300() {
		ExplodeParser p = new ExplodeParser();
		p.setQueryParserService(queryParserService);
		String query = "explode ";

		try {
			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("20300", e.getType());
			assertEquals(8, e.getStartOffset());
			assertEquals(7, e.getEndOffset());	
		}
	}


}
