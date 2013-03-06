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
}
