/*
 * Copyright 2013 Future Systems
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.araqne.logdb.query.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.FunctionRegistry;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.impl.FunctionRegistryImpl;
import org.junit.Test;

public class QueryTokenizerTest {
	private FunctionRegistry functionRegistry = new FunctionRegistryImpl();

	@Test
	public void testParseSubQuery() {
		String q = "table iis | join ip [ table users | fields user_id, ip ]";
		List<String> commands = QueryTokenizer.parseCommands(q);
		assertEquals(2, commands.size());
	}

	@Test
	public void testParseNonSubQuery() {
		String q = "table iis | search == \"*|[*\"";
		List<String> commands = QueryTokenizer.parseCommands(q);
		assertEquals(2, commands.size());
	}

	@Test
	public void testParseCommands() {
		String q = "table limit=1000000 local\\arko-guro | search sip contain \"10.1.\" | stats count by sip";
		List<String> commands = QueryTokenizer.parseCommands(q);
		assertEquals(3, commands.size());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testOptions() {
		String query = "textfile offset=1 limit=10 sample.log";

		ParseResult r = QueryTokenizer.parseOptions(null, query, "textfile".length(), Arrays.asList("offset", "limit"),
				functionRegistry);
		Map<String, String> options = (Map<String, String>) r.value;

		assertEquals("1", options.get("offset"));
		assertEquals("10", options.get("limit"));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testOptions2() {
		String query = "search offset=1 limit=10 1 == 1";

		ParseResult r = QueryTokenizer.parseOptions(null, query, "search".length(), Arrays.asList("offset", "limit"),
				functionRegistry);
		Map<String, String> options = (Map<String, String>) r.value;

		assertEquals("1", options.get("offset"));
		assertEquals("10", options.get("limit"));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testOptions3() {
		String query = "search offset=1 limit=10 in(field,\"?&key=value\")";

		ParseResult r = QueryTokenizer.parseOptions(null, query, "search".length(), Arrays.asList("offset", "limit"),
				functionRegistry);
		Map<String, String> options = (Map<String, String>) r.value;

		assertEquals("1", options.get("offset"));
		assertEquals("10", options.get("limit"));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testOptions4() {
		String query = "search in(field,\"?&key=value\")";

		ParseResult r = QueryTokenizer.parseOptions(null, query, "search".length(), Arrays.asList("offset", "limit"),
				functionRegistry);
		Map<String, String> options = (Map<String, String>) r.value;
		assertEquals(0, options.size());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testOptions5() {
		String query = "search offset=1 limit=10";

		ParseResult r = QueryTokenizer.parseOptions(null, query, "search".length(), Arrays.asList("offset", "limit"),
				functionRegistry);
		Map<String, String> options = (Map<String, String>) r.value;

		assertEquals("1", options.get("offset"));
		assertEquals("10", options.get("limit"));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testOptions7() {
		String query = "textfile parser=\"<key = value>\" foo.txt";

		ParseResult r = QueryTokenizer.parseOptions(null, query, "textfile".length(), Arrays.asList("parser"), functionRegistry);
		Map<String, String> options = (Map<String, String>) r.value;
		assertEquals("<key = value>", options.get("parser"));
	}

	@Test
	public void testError90000() { 
		String query = "search offset = 1  limit=10 ";

		try {
			QueryTokenizer.parseOptions(null, query, "search".length(), Arrays.asList("offset", "limit"), functionRegistry);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("90000", e.getType());
			assertEquals(13, e.getOffsetS());
			assertEquals(13, e.getOffsetE());
		}
	}
	
	@Test
	public void testError90001() { 
		String query = "textfile limit=100 \"parser\"=\"<key = value>\" foo.txt";

		try {
			QueryTokenizer.parseOptions(null, query, "textfile".length(), Arrays.asList("parser", "limit"), functionRegistry);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("90001", e.getType());
			assertEquals(19, e.getOffsetS());
			assertEquals(26, e.getOffsetE());
		}
	}

	@Test
	public void testError90002() {
		String query = "textfile limit=100 parser=\"<key = value> foo.txt";

		try {
			QueryTokenizer.parseOptions(null, query, "textfile".length(), Arrays.asList("parser", "limit"), functionRegistry);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("90002", e.getType());
			assertEquals(26, e.getOffsetS());
			assertEquals(47, e.getOffsetE());
		}
	}

	@Test
	public void testCsvParse() {
		int p = QueryTokenizer.findKeyword("sum(min(1, 2))", ",");
		assertEquals(-1, p);

		p = QueryTokenizer.findKeyword("sum(min(1, 2)), 1", ",");
		assertEquals(14, p);
	}

	@Test
	public void testQuote() {
		String s = "table limit=1 iis | rex field=line \"(?<d>\\\\d+-\\\\d+-\\\\d+)\" | eval d2 = date(d, \"yyyy-MM-dd HH:mm:ss\") | fields d, d2";
		List<String> commands = QueryTokenizer.parseCommands(s);
		System.out.println(commands.get(1));
	}

	@Test
	public void testError90003() {
		String query = "table iis |    |";

		try {
			QueryTokenizer.parseCommands(query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("90003", e.getType());
			assertEquals(15, e.getOffsetS());
			assertEquals(15, e.getOffsetE());
		}

		query = "table test |    ";

		try {
			QueryTokenizer.parseCommands(query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("90003", e.getType());
			assertEquals(15, e.getOffsetS());
			assertEquals(15, e.getOffsetE());
		}
	}
	
	@Test
	public void testError90004(){
		String query = "";
		
		try {
			QueryTokenizer.nextString(query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("90004", e.getType());
			assertEquals(0, e.getOffsetS());
			assertEquals(0, e.getOffsetE());
		}
	}
	
	@Test
	public void testError90005(){
		String query = "\"table test";
		
		try {
			QueryTokenizer.nextString(query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("90005", e.getType());
			assertEquals(query, e.getParams().get("value"));
		}
		
	}
}
