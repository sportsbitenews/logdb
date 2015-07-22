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

import java.util.Calendar;
import java.util.Date;

import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.Row;
import org.araqne.logdb.TimeSpan;
import org.araqne.logdb.TimeUnit;
import org.araqne.logdb.query.aggregator.AggregationField;
import org.araqne.logdb.query.command.Timechart;
import org.araqne.logdb.query.engine.QueryParserServiceImpl;
import org.araqne.logdb.query.expr.Expression;
import org.junit.Test;

public class TimechartParserTest {
	private QueryParserService queryParserService = new QueryParserServiceImpl();
	
	@Test
	public void testInsufficientCommand() {
		TimechartParser p = new TimechartParser();
		p.setQueryParserService(queryParserService);
		String query = "timecahrt";
		
		try {
			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("21800", e.getType());
			assertEquals(10, e.getStartOffset());
			assertEquals(8, e.getEndOffset());
		}
	}

	@Test
	public void testMostSimpleCase() {
		TimechartParser p = new TimechartParser();
		p.setQueryParserService(queryParserService);
		
		Timechart tc = (Timechart) p.parse(null, "timechart count");

		assertEquals(1, tc.getAggregationFields().size());
		assertEquals("count", tc.getAggregationFields().get(0).getName());
		assertEquals(1, tc.getTimeSpan().amount);
		assertEquals(TimeUnit.Day, tc.getTimeSpan().unit);
		assertEquals("timechart span=1d count", tc.toString());
	}

	@Test
	public void testCount() {
		TimechartParser p = new TimechartParser();
		p.setQueryParserService(queryParserService);
		
		Timechart tc = (Timechart) p.parse(null, "timechart span=1d count");

		assertEquals(1, tc.getAggregationFields().size());
		assertEquals("count", tc.getAggregationFields().get(0).getName());
		assertEquals(1, tc.getTimeSpan().amount);
		assertEquals(TimeUnit.Day, tc.getTimeSpan().unit);
		assertEquals("timechart span=1d count", tc.toString());
	}

	@Test
	public void testCountWithClause() {
		TimechartParser p = new TimechartParser();
		p.setQueryParserService(queryParserService);
		
		Timechart tc = (Timechart) p.parse(null, "timechart span=1d count by sip");

		assertEquals(1, tc.getAggregationFields().size());
		assertEquals("count", tc.getAggregationFields().get(0).getName());
		assertEquals(1, tc.getTimeSpan().amount);
		assertEquals(TimeUnit.Day, tc.getTimeSpan().unit);
		assertEquals("sip", tc.getKeyField());
		assertEquals("timechart span=1d count by sip", tc.toString());
	}

	@Test
	public void testNestedSum() {
		TimechartParser p = new TimechartParser();
		p.setQueryParserService(queryParserService);
		
		Timechart tc = (Timechart) p.parse(null, "timechart span=1m sum(sport / 2)");

		AggregationField agg = tc.getAggregationFields().get(0);
		assertEquals(1, tc.getAggregationFields().size());
		assertEquals("sum((sport / 2))", agg.getName());

		Expression arg1 = agg.getFunction().getArguments().get(0);
		Row m = new Row();
		m.put("sport", 100);
		assertEquals(50.0, arg1.eval(m));
		assertEquals("timechart span=1m sum((sport / 2))", tc.toString());
	}

	@Test
	public void test2MonthBucketing() {
		TimeSpan mon2 = new TimeSpan(2, TimeUnit.Month);
		assertEquals(date(2013, 1, 1), TimeUnit.getKey(date(2013, 1, 2), mon2));
		assertEquals(date(2013, 1, 1), TimeUnit.getKey(date(2013, 2, 2), mon2));
		assertEquals(date(2013, 3, 1), TimeUnit.getKey(date(2013, 3, 2), mon2));
		assertEquals(date(2013, 3, 1), TimeUnit.getKey(date(2013, 4, 2), mon2));
		assertEquals(date(2013, 5, 1), TimeUnit.getKey(date(2013, 5, 2), mon2));
		assertEquals(date(2013, 5, 1), TimeUnit.getKey(date(2013, 6, 2), mon2));
		assertEquals(date(2013, 7, 1), TimeUnit.getKey(date(2013, 7, 2), mon2));
		assertEquals(date(2013, 7, 1), TimeUnit.getKey(date(2013, 8, 2), mon2));
		assertEquals(date(2013, 9, 1), TimeUnit.getKey(date(2013, 9, 2), mon2));
		assertEquals(date(2013, 9, 1), TimeUnit.getKey(date(2013, 10, 2), mon2));
		assertEquals(date(2013, 11, 1), TimeUnit.getKey(date(2013, 11, 2), mon2));
		assertEquals(date(2013, 11, 1), TimeUnit.getKey(date(2013, 12, 2), mon2));
	}

	@Test
	public void test3MonthBucketing() {
		TimeSpan mon3 = new TimeSpan(3, TimeUnit.Month);
		assertEquals(date(2013, 1, 1), TimeUnit.getKey(date(2013, 1, 2), mon3));
		assertEquals(date(2013, 1, 1), TimeUnit.getKey(date(2013, 2, 2), mon3));
		assertEquals(date(2013, 1, 1), TimeUnit.getKey(date(2013, 3, 2), mon3));
		assertEquals(date(2013, 4, 1), TimeUnit.getKey(date(2013, 4, 2), mon3));
		assertEquals(date(2013, 4, 1), TimeUnit.getKey(date(2013, 5, 2), mon3));
		assertEquals(date(2013, 4, 1), TimeUnit.getKey(date(2013, 6, 2), mon3));
		assertEquals(date(2013, 7, 1), TimeUnit.getKey(date(2013, 7, 2), mon3));
		assertEquals(date(2013, 7, 1), TimeUnit.getKey(date(2013, 8, 2), mon3));
		assertEquals(date(2013, 7, 1), TimeUnit.getKey(date(2013, 9, 2), mon3));
		assertEquals(date(2013, 10, 1), TimeUnit.getKey(date(2013, 10, 2), mon3));
		assertEquals(date(2013, 10, 1), TimeUnit.getKey(date(2013, 11, 2), mon3));
		assertEquals(date(2013, 10, 1), TimeUnit.getKey(date(2013, 12, 2), mon3));
	}

	@Test
	public void test4MonthBucketing() {
		TimeSpan mon4 = new TimeSpan(4, TimeUnit.Month);
		assertEquals(date(2013, 1, 1), TimeUnit.getKey(date(2013, 1, 2), mon4));
		assertEquals(date(2013, 1, 1), TimeUnit.getKey(date(2013, 2, 2), mon4));
		assertEquals(date(2013, 1, 1), TimeUnit.getKey(date(2013, 3, 2), mon4));
		assertEquals(date(2013, 1, 1), TimeUnit.getKey(date(2013, 4, 2), mon4));
		assertEquals(date(2013, 5, 1), TimeUnit.getKey(date(2013, 5, 2), mon4));
		assertEquals(date(2013, 5, 1), TimeUnit.getKey(date(2013, 6, 2), mon4));
		assertEquals(date(2013, 5, 1), TimeUnit.getKey(date(2013, 7, 2), mon4));
		assertEquals(date(2013, 5, 1), TimeUnit.getKey(date(2013, 8, 2), mon4));
		assertEquals(date(2013, 9, 1), TimeUnit.getKey(date(2013, 9, 2), mon4));
		assertEquals(date(2013, 9, 1), TimeUnit.getKey(date(2013, 10, 2), mon4));
		assertEquals(date(2013, 9, 1), TimeUnit.getKey(date(2013, 11, 2), mon4));
		assertEquals(date(2013, 9, 1), TimeUnit.getKey(date(2013, 12, 2), mon4));
	}

	@Test
	public void test6MonthBucketing() {
		TimeSpan mon6 = new TimeSpan(6, TimeUnit.Month);
		assertEquals(date(2013, 1, 1), TimeUnit.getKey(date(2013, 1, 2), mon6));
		assertEquals(date(2013, 1, 1), TimeUnit.getKey(date(2013, 2, 2), mon6));
		assertEquals(date(2013, 1, 1), TimeUnit.getKey(date(2013, 3, 2), mon6));
		assertEquals(date(2013, 1, 1), TimeUnit.getKey(date(2013, 4, 2), mon6));
		assertEquals(date(2013, 1, 1), TimeUnit.getKey(date(2013, 5, 2), mon6));
		assertEquals(date(2013, 1, 1), TimeUnit.getKey(date(2013, 6, 2), mon6));
		assertEquals(date(2013, 7, 1), TimeUnit.getKey(date(2013, 7, 2), mon6));
		assertEquals(date(2013, 7, 1), TimeUnit.getKey(date(2013, 8, 2), mon6));
		assertEquals(date(2013, 7, 1), TimeUnit.getKey(date(2013, 9, 2), mon6));
		assertEquals(date(2013, 7, 1), TimeUnit.getKey(date(2013, 10, 2), mon6));
		assertEquals(date(2013, 7, 1), TimeUnit.getKey(date(2013, 11, 2), mon6));
		assertEquals(date(2013, 7, 1), TimeUnit.getKey(date(2013, 12, 2), mon6));
	}

	@Test
	public void test1YearBucketing() {
		TimeSpan mon6 = new TimeSpan(1, TimeUnit.Year);
		assertEquals(date(2013, 1, 1), TimeUnit.getKey(date(2013, 1, 2), mon6));
		assertEquals(date(2013, 1, 1), TimeUnit.getKey(date(2013, 2, 2), mon6));
		assertEquals(date(2013, 1, 1), TimeUnit.getKey(date(2013, 3, 2), mon6));
		assertEquals(date(2013, 1, 1), TimeUnit.getKey(date(2013, 4, 2), mon6));
		assertEquals(date(2013, 1, 1), TimeUnit.getKey(date(2013, 5, 2), mon6));
		assertEquals(date(2013, 1, 1), TimeUnit.getKey(date(2013, 6, 2), mon6));
		assertEquals(date(2014, 1, 1), TimeUnit.getKey(date(2014, 7, 2), mon6));
		assertEquals(date(2014, 1, 1), TimeUnit.getKey(date(2014, 8, 2), mon6));
		assertEquals(date(2014, 1, 1), TimeUnit.getKey(date(2014, 9, 2), mon6));
		assertEquals(date(2014, 1, 1), TimeUnit.getKey(date(2014, 10, 2), mon6));
		assertEquals(date(2014, 1, 1), TimeUnit.getKey(date(2014, 11, 2), mon6));
		assertEquals(date(2014, 1, 1), TimeUnit.getKey(date(2014, 12, 2), mon6));
	}

	private Date date(int year, int mon, int day) {
		Calendar c = Calendar.getInstance();
		c.set(Calendar.YEAR, year);
		c.set(Calendar.MONTH, mon - 1);
		c.set(Calendar.DAY_OF_MONTH, day);
		c.set(Calendar.HOUR_OF_DAY, 0);
		c.set(Calendar.MINUTE, 0);
		c.set(Calendar.SECOND, 0);
		c.set(Calendar.MILLISECOND, 0);
		return c.getTime();
	}
}
