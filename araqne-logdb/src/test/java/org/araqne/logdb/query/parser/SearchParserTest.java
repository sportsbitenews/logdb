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

import static org.junit.Assert.*;

import org.junit.Test;
import org.araqne.logdb.LogMap;
import org.araqne.logdb.LogQueryCommand;
import org.araqne.logdb.LogQueryParseException;
import org.araqne.logdb.query.command.Search;
import org.araqne.logdb.query.expr.Expression;

public class SearchParserTest {
	@Test
	public void testWildSearch() {
		SearchParser p = new SearchParser();
		Search search = (Search) p.parse(null, "search sip == \"10.1.*\" ");
		Expression expr = search.getExpression();

		LogMap map = new LogMap();
		map.put("sip", "10.1.2.2");
		assertTrue((Boolean) expr.eval(map));

		map = new LogMap();
		map.put("sip", "10.2.2.2");
		assertFalse((Boolean) expr.eval(map));
	}

	// iisidx.i1 == "74.86.*" or iisidx.i1 == "211.*" or iisidx.i1 ==
	// "110.221.*"
	@Test
	public void testWhitespace() {
		SearchParser p = new SearchParser();
		Search search = (Search) p.parse(null,
				"search sip == \"74.86.*\"  \tor     sip  ==   \"211.*\"      or\tsip == \"110.221.*\"");
		Expression expr = search.getExpression();

		LogMap map = new LogMap();
		map.put("sip", "74.86.1.2");
		assertTrue((Boolean) expr.eval(map));

		map = new LogMap();
		map.put("sip", "211.123.1.1");
		assertTrue((Boolean) expr.eval(map));

		map = new LogMap();
		map.put("sip", "110.221.3.4");
		assertTrue((Boolean) expr.eval(map));

		map = new LogMap();
		map.put("sip", "10.2.2.2");
		assertFalse((Boolean) expr.eval(map));
	}

	@Test
	public void testLimit() {
		SearchParser p = new SearchParser();
		Search search = (Search) p.parse(null, "search limit=10 port > 1024");
		Expression expr = search.getExpression();

		LogMap map = new LogMap();
		map.put("port", 1024);
		assertFalse((Boolean) expr.eval(map));

		map.put("port", 1025);
		assertTrue((Boolean) expr.eval(map));

		assertEquals(10L, (long) search.getLimit());
	}

	@Test
	public void testLimitOnly() {
		SearchParser p = new SearchParser();
		Search search = (Search) p.parse(null, "search limit=10");
		assertEquals(10, (long) search.getLimit());
	}

	@Test
	public void testMissingEscape() {
		try {
			String query = "search limit=10 category == \"E002\" and ((method == \"<iframe src=\"http://www.w3schools.com\">,,,,,,\"\",,\"<\"<<<\"<,,</iframe>\"))";
			SearchParser p = new SearchParser();
			p.parse(null, query);
			fail();
		} catch (LogQueryParseException e) {
			assertEquals("quote-mismatch", e.getType());
		}
	}

	@Test
	public void testEscape() {
		String query = "search limit=10 category == \"E002\" and ((method == \"<iframe src=\\\"http://www.w3schools.com\\\">,,,,,,\\\"\\\",,\\\"<\\\"<<<\\\"<,,</iframe>\"))";
		SearchParser p = new SearchParser();
		Search search = (Search) p.parse(null, query);
		Output output = new Output();
		search.setNextCommand(output);

		LogMap m = new LogMap();
		m.put("category", "E002");
		m.put("method", "<iframe src=\"http://www.w3schools.com\">,,,,,,\"\",,\"<\"<<<\"<,,</iframe>");
		search.push(m);
		assertEquals(m, output.m);

	}

	@Test
	public void testEscape2() {
		String query = "search in (method,\"<iframe src=\\\"http://www.w3schools.com\\\">,,,,,,\\\"\\\",,\\\"<\\\"<<<\\\"<,,</iframe>\") and d_port == \"21\"";
		SearchParser p = new SearchParser();
		Search search = (Search) p.parse(null, query);
		System.out.println(search.getExpression());
		Output output = new Output();
		search.setNextCommand(output);

		LogMap m = new LogMap();
		m.put("d_port", "21");
		m.put("method", "<iframe src=\"http://www.w3schools.com\">,,,,,,\"\",,\"<\"<<<\"<,,</iframe>");
		search.push(m);
		assertEquals(m, output.m);

	}

	private class Output extends LogQueryCommand {
		private LogMap m;

		@Override
		public void push(LogMap m) {
			this.m = m;
		}

		@Override
		public boolean isReducer() {
			return false;
		}

	}
}
