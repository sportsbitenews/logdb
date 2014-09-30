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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.araqne.log.api.LogParserRegistry;
import org.araqne.log.api.impl.LogParserRegistryImpl;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.impl.FunctionRegistryImpl;
import org.araqne.logdb.query.engine.QueryParserServiceImpl;
import org.araqne.logstorage.LogTableRegistry;
import org.araqne.logstorage.TableSchema;
import org.junit.Before;
import org.junit.Test;

public class ParseParserTest {
	private QueryParserService queryParserService;

	@Before
	public void setup() {
		QueryParserServiceImpl p = new QueryParserServiceImpl();
		p.setFunctionRegistry(new FunctionRegistryImpl());
		queryParserService = p;
	}
	
	@Test
	public void testError21000(){
		ParseParser parser = new ParseParser(null);
		parser.setQueryParserService(queryParserService);
		String query = "parse ";
		
		try {
			parser.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("21000", e.getType());
			assertEquals(6, e.getOffsetS());
			assertEquals(5, e.getOffsetE());	
		}
	}
	
	@Test
	public void testError21001(){
		ParseParser parser = new ParseParser( mock(LogParserRegistryImpl.class));
		parser.setQueryParserService(queryParserService);
		String query = "parse delim";
		try {
			parser.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("21001", e.getType());
			assertEquals(6, e.getOffsetS());
			assertEquals(11, e.getOffsetE());	
		}
	}
}
