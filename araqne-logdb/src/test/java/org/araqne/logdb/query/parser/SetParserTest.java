package org.araqne.logdb.query.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParserService;
import org.junit.Test;

public class SetParserTest {
	
	private QueryParserService queryParserService;

	@Test
	public void testError10400(){
		SetParser p = new SetParser();
		
		String query = "set to \"now\"";
		
		try {
			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("10400", e.getType());
			assertEquals(4, e.getStartOffset());
			assertEquals(11, e.getEndOffset());	
		}
	}
	
	@Test
	public void testError10401(){
		SetParser p = new SetParser();
		p.setQueryParserService(queryParserService);
		String query = "set  =\"now\"";
		
		try {
			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("10401", e.getType());
			assertEquals(4, e.getStartOffset());
			assertEquals(4, e.getEndOffset());	
		}
	}

	@Test
	public void testError10402(){
		SetParser p = new SetParser();
		String query = "set to=";
		
		try {
			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("10402", e.getType());
			assertEquals(7, e.getStartOffset());
			assertEquals(6, e.getEndOffset());	
		}
	}
}
