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

import org.junit.Before;
import org.junit.Test;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.impl.FunctionRegistryImpl;
import org.araqne.logdb.query.command.Sort;
import org.araqne.logdb.query.engine.QueryParserServiceImpl;

import static org.junit.Assert.*;

public class SortParserTest {
	private QueryParserService queryParserService;

	@Before
	public void setup() {
		QueryParserServiceImpl p = new QueryParserServiceImpl();
		p.setFunctionRegistry(new FunctionRegistryImpl());
		queryParserService = p;
	}
	
	@Test
	public void testSingleColumn() {
		String command = "sort field1";

		SortParser p = new SortParser();
		p.setQueryParserService(queryParserService);
		
		Sort sort = (Sort) p.parse(null, command);
		assertEquals(1, sort.getFields().length);
		assertEquals("field1", sort.getFields()[0].getName());
		assertEquals(true, sort.getFields()[0].isAsc());
		assertNull(sort.getLimit());
		assertEquals("sort field1", sort.toString());
	}

	@Test
	public void testMultiColumns() {
		String command = "sort field1,field2, field3";

		SortParser p = new SortParser();
		p.setQueryParserService(queryParserService);
		
		Sort sort = (Sort) p.parse(null, command);
		assertEquals(3, sort.getFields().length);
		assertEquals("field1", sort.getFields()[0].getName());
		assertEquals(true, sort.getFields()[0].isAsc());
		assertEquals("field2", sort.getFields()[1].getName());
		assertEquals(true, sort.getFields()[1].isAsc());
		assertEquals("field3", sort.getFields()[2].getName());
		assertEquals(true, sort.getFields()[2].isAsc());
		assertNull(sort.getLimit());
		assertEquals("sort field1, field2, field3", sort.toString());
	}

	@Test
	public void testOrder() {
		String command = "sort -field1,+field2, field3";

		SortParser p = new SortParser();
		p.setQueryParserService(queryParserService);
		
		Sort sort = (Sort) p.parse(null, command);
		assertEquals(3, sort.getFields().length);
		assertEquals("field1", sort.getFields()[0].getName());
		assertEquals(false, sort.getFields()[0].isAsc());
		assertEquals("field2", sort.getFields()[1].getName());
		assertEquals(true, sort.getFields()[1].isAsc());
		assertEquals("field3", sort.getFields()[2].getName());
		assertEquals(true, sort.getFields()[2].isAsc());
		assertNull(sort.getLimit());
		assertEquals("sort -field1, field2, field3", sort.toString());
	}

	@Test
	public void testLimitAndSingleColumn() {
		String command = "sort limit=10 -field1";

		SortParser p = new SortParser();
		p.setQueryParserService(queryParserService);
		
		Sort sort = (Sort) p.parse(null, command);
		assertEquals(1, sort.getFields().length);
		assertEquals("field1", sort.getFields()[0].getName());
		assertEquals(false, sort.getFields()[0].isAsc());
		assertEquals(10, (int) sort.getLimit());
		assertEquals("sort limit=10 -field1", sort.toString());
	}

	@Test
	public void testComplexCase() {
		String command = "sort limit=20 field1, +field2,-field3";

		SortParser p = new SortParser();
		p.setQueryParserService(queryParserService);
		
		Sort sort = (Sort) p.parse(null, command);
		assertEquals(3, sort.getFields().length);

		assertEquals("field1", sort.getFields()[0].getName());
		assertEquals(true, sort.getFields()[0].isAsc());

		assertEquals("field2", sort.getFields()[1].getName());
		assertEquals(true, sort.getFields()[1].isAsc());

		assertEquals("field3", sort.getFields()[2].getName());
		assertEquals(false, sort.getFields()[2].isAsc());

		assertEquals(20, (int) sort.getLimit());
		assertEquals("sort limit=20 field1, field2, -field3", sort.toString());
	}

	@Test
	public void testBrokenCase() {
		String command = "sort limit=20";

		try {
			SortParser p = new SortParser();
			p.setQueryParserService(queryParserService);
			p.parse(null, command);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + command);
				System.out.println(e.getMessage());
			}
			assertEquals("21600", e.getType());
			assertEquals(13, e.getStartOffset());
			assertEquals(12, e.getEndOffset());	
		}
	}

	@Test
	public void testBrokenCase2() {
		String command = "sort ";

		try {
			SortParser p = new SortParser();
			p.setQueryParserService(queryParserService);
			p.parse(null, command);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + command);
				System.out.println(e.getMessage());
			}
			assertEquals("21600", e.getType());
			assertEquals(5, e.getStartOffset());
			assertEquals(4, e.getEndOffset());	
		}
	}
}
