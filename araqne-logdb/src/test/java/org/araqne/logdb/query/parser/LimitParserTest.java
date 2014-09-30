package org.araqne.logdb.query.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.Limit;
import org.junit.Test;

public class LimitParserTest {
	
	@Test
	public void testLimitOnly() {
		LimitParser parser = new LimitParser();
		Limit limit = (Limit) parser.parse(null, "limit 10");
		assertEquals(0, limit.getOffset());
		assertEquals(10, limit.getLimit());
	}

	@Test
	public void testOffsetAndLimit() {
		LimitParser parser = new LimitParser();
		Limit limit = (Limit) parser.parse(null, "limit 5 10");
		assertEquals(5, limit.getOffset());
		assertEquals(10, limit.getLimit());
	}
	
	@Test
	public void testError20600(){
		LimitParser p = new LimitParser();
		String query = "limit  ";
		
		try {
			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("20600", e.getType());
			assertEquals(6, e.getOffsetS());
			assertEquals(6, e.getOffsetE());	
		}
		
		query = "limit 5 10 15";
		try {
			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("20600", e.getType());
			assertEquals(6, e.getOffsetS());
			assertEquals(12, e.getOffsetE());	
		}
	}
	
	@Test
	public void testError20601(){
		LimitParser p = new LimitParser();
		String query = "limit 1 a";

		try {
			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("20601", e.getType());
			assertEquals(6, e.getOffsetS());
			assertEquals(8, e.getOffsetE());	
		}
	}
}
