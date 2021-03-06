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

import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.impl.FunctionRegistryImpl;
import org.araqne.logdb.query.aggregator.AggregationField;
import org.araqne.logdb.query.command.Stats;
import org.araqne.logdb.query.engine.QueryParserServiceImpl;
import org.junit.Before;
import org.junit.Test;

public class StatsParserTest {
	private QueryParserService queryParserService;

	@Before
	public void setup() {
		QueryParserServiceImpl p = new QueryParserServiceImpl();
		p.setFunctionRegistry(new FunctionRegistryImpl());
		queryParserService = p;
	}

	@Test
	public void testCount() {
		StatsParser p = new StatsParser();
		p.setQueryParserService(queryParserService);

		Stats stats = (Stats) p.parse(null, "stats count");
		assertEquals(1, stats.getAggregationFields().size());
		assertEquals("count", stats.getAggregationFields().get(0).getName());
		assertEquals("stats count", stats.toString());

		// abbreviation form
		stats = (Stats) p.parse(null, "stats c");
		assertEquals(1, stats.getAggregationFields().size());
		assertEquals("count", stats.getAggregationFields().get(0).getName());
		assertEquals("stats count", stats.toString());
	}

	@Test
	public void testCountArgs(){
		StatsParser p = new StatsParser();
		p.setQueryParserService(queryParserService);
		String query = "stats count(1,2) by line";
		try {
			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if (e.isDebugMode()) {
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("91010", e.getType());
			assertEquals(6, e.getStartOffset());
			assertEquals(15, e.getEndOffset());	
		}
	}	
	
	@Test
	public void testFirstArgs(){
		StatsParser p = new StatsParser();
		p.setQueryParserService(queryParserService);
		String query = "stats first(1,2) ";
		try {
			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if (e.isDebugMode()) {
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("91020", e.getType());
			assertEquals(6, e.getStartOffset());
			assertEquals(15, e.getEndOffset());	
		}
	}
	
	@Test
	public void testSumMin() {
		StatsParser p = new StatsParser();
		p.setQueryParserService(queryParserService);

		Stats stats = (Stats) p.parse(null, "stats sum(min(10000, sport)) as foo");
		AggregationField field = stats.getAggregationFields().get(0);
		assertEquals("foo", field.getName());
		assertEquals("stats sum(min(10000, sport)) as foo", stats.toString());
	}

	@Test
	public void testSingleClauses() {
		StatsParser p = new StatsParser();
		p.setQueryParserService(queryParserService);

		Stats stats = (Stats) p.parse(null, "stats sum(rcvd) by sip");
		assertEquals(1, stats.getAggregationFields().size());
		assertEquals("sum(rcvd)", stats.getAggregationFields().get(0).getName());
		assertEquals("stats sum(rcvd) by sip", stats.toString());
	}

	@Test
	public void testMultiAggregationsAndClauses() {
		StatsParser p = new StatsParser();
		p.setQueryParserService(queryParserService);

		Stats stats = (Stats) p.parse(null, "stats sum(rcvd) as rcvd, sum(sent) as sent by sip, dip");
		assertEquals(2, stats.getAggregationFields().size());
		assertEquals(2, stats.getClauses().size());
		assertEquals("rcvd", stats.getAggregationFields().get(0).getName());
		assertEquals("sent", stats.getAggregationFields().get(1).getName());
		assertEquals("sip", stats.getClauses().get(0));
		assertEquals("dip", stats.getClauses().get(1));
		assertEquals("stats sum(rcvd) as rcvd, sum(sent) as sent by sip, dip", stats.toString());
	}

	@Test
	public void testNeedFunction() {
		StatsParser p = new StatsParser();
		p.setQueryParserService(queryParserService);
		String query = "stats";
		try {
			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("21700", e.getType());
			assertEquals(6, e.getStartOffset());
			assertEquals(4, e.getEndOffset());	
		}
	}
	
	@Test
	public void testMissingClause() {
		StatsParser p = new StatsParser();
		p.setQueryParserService(queryParserService);
		String query = "stats sum(rcvd) as rcvd, sum(sent) as sent by sip,";
		try {
			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("21701", e.getType());
			assertEquals(6, e.getStartOffset());
			assertEquals(49, e.getEndOffset());	
		}
	}
	
	@Test
	public void testAggreGationFunction() {
		StatsParser p = new StatsParser();
		p.setQueryParserService(queryParserService);
		String query = "stats sample";
		try {
			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("21702", e.getType());
			assertEquals(6, e.getStartOffset());
			assertEquals(11,  e.getEndOffset());	
		}
	}
	
	@Test
	public void testCreateAggreGationFunction() {
		StatsParser p = new StatsParser();
		p.setQueryParserService(queryParserService);
		String query = "stats sum";
		try {
			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("21703", e.getType());
			assertEquals(6, e.getStartOffset());
			assertEquals(8,  e.getEndOffset());	
		}
	}

	@Test
	public void testWhiteSpaceFieldNameBugFix() {
		StatsParser p = new StatsParser();
		p.setQueryParserService(queryParserService);
		Stats stats = (Stats) p.parse(null, "stats first(a) as a , first(b) as b by c");
		assertEquals("a", stats.getAggregationFields().get(0).getName());
		assertEquals("b", stats.getAggregationFields().get(1).getName());
		assertEquals("c", stats.getClauses().get(0));
	}

	// araqne/issue#684. check also boxplot and timechart.
	@Test
	public void testMultilineBugFix() {
		StatsParser p = new StatsParser();
		p.setQueryParserService(queryParserService);
		Stats stats = (Stats) p.parse(null, "stats count\nby\nname");
		assertEquals("count", stats.getAggregationFields().get(0).getName());
		assertEquals("name", stats.getClauses().get(0));
	}
}
