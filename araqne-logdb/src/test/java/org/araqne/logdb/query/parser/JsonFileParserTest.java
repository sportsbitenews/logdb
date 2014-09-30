package org.araqne.logdb.query.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.impl.FunctionRegistryImpl;
import org.araqne.logdb.query.engine.QueryParserServiceImpl;
import org.junit.Before;
import org.junit.Test;

public class JsonFileParserTest {
	private QueryParserService queryParserService;
	
	@Before
	public void setup() {
		QueryParserServiceImpl p = new QueryParserServiceImpl();
		p.setFunctionRegistry(new FunctionRegistryImpl());
		queryParserService = p;
	}
	
	@Test
	public void testError10900() {
		JsonFileParser p = new JsonFileParser(null);
		p.setQueryParserService(queryParserService);
		String query = "jsonfile limit=100 JsonFile.txt";	
		
		try {
			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("10900", e.getType());
			assertEquals(19, e.getOffsetS());
			assertEquals(30, e.getOffsetE());
		}
	}
}
