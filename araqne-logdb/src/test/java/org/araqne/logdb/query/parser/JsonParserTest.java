package org.araqne.logdb.query.parser;

import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.Json;
import org.junit.Test;

import static org.junit.Assert.*;

public class JsonParserTest {
	@Test
	public void testJson() {
		String JSON_QUERY = "json \"[{'id':  1}, {'id':2}]\"";

		JsonParser parser = new JsonParser();
		Json json = (Json) parser.parse(null, JSON_QUERY);
		assertEquals(2, json.getLogs().size());
		assertEquals(1, json.getLogs().get(0).get("id"));
		assertEquals(2, json.getLogs().get(1).get("id"));

		// test query string re-generation
		assertEquals(JSON_QUERY, json.toString());
	}

	@Test
	public void testJsonErr10200(){
		String query = "json";
		JsonParser parser = new JsonParser();
		
		try {
			parser.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("10200", e.getType());
			assertEquals(5, e.getOffsetS());
			assertEquals(3, e.getOffsetE());
		}

		query = "json \"{}";

		try {
			parser.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("10200", e.getType());
			assertEquals(5, e.getOffsetS());
			assertEquals(7, e.getOffsetE());
		}

		query = "json    {}";
		try {
			parser.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("10200", e.getType());
			assertEquals(5, e.getOffsetS());
			assertEquals(9, e.getOffsetE());
		}

		query = "json    \"{}";
		try {
			parser.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("10200", e.getType());
			assertEquals(5, e.getOffsetS());
			assertEquals(10, e.getOffsetE());
		}
	}

	@Test
	public void testJsonErr10201(){
		String query = "json \"test\"\"\"";
		JsonParser parser = new JsonParser();

		try {
			parser.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("10201", e.getType());
			assertEquals(5, e.getOffsetS());
			assertEquals(12, e.getOffsetE());
		}
	}


	@Test
	public void testJsonErr10202(){
		String query = "json \"{test}\"";
		JsonParser parser = new JsonParser();

		try {
			parser.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("10202", e.getType());
			assertEquals(5, e.getOffsetS());
			assertEquals(12, e.getOffsetE());
		}
	}
}
