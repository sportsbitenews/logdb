package org.araqne.logdb.query.parser;

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
}
