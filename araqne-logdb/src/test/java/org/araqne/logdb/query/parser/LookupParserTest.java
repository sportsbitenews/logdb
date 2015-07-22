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
import org.araqne.logdb.impl.LookupHandlerRegistryImpl;
import org.araqne.logdb.query.command.Lookup;
import org.junit.Test;

public class LookupParserTest {
	@Test
	public void testSimpleCase() {
		// lookup <name> <lookup key field> OUTPUT <lookup target field>
		LookupParser p = new LookupParser(null);
		Lookup lookup = (Lookup) p.parse(null, "lookup sample code output auth_code_desc");
		assertEquals("sample", lookup.getHandlerName());
		assertEquals("code", lookup.getSourceField());
		assertEquals("code", lookup.getLookupInputField());
		assertEquals("auth_code_desc", lookup.getOutputFields().get("auth_code_desc"));
	}

	@Test
	public void testFullArgs() {
		// lookup <name> <lookup key field> AS <renamed field> OUTPUT <lookup
		// target field> AS <renamed target name>
		LookupParser p = new LookupParser(null);
		Lookup lookup = (Lookup) p.parse(null, "lookup sample code AS in OUTPUT auth_code_desc as out");
		assertEquals("sample", lookup.getHandlerName());
		assertEquals("code", lookup.getSourceField());
		assertEquals("in", lookup.getLookupInputField());
		assertEquals("out", lookup.getOutputFields().get("auth_code_desc"));
	}
	
	@Test
	public void testError20700(){
		LookupParser p = new LookupParser(null);
		String query = "lookup sample code AS in auth_code_desc as out";
	
		try {
			 p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("20700", e.getType());
			assertEquals(7, e.getStartOffset());
			assertEquals(45, e.getEndOffset());	
		}
	}
	
	@Test
	public void testError20701(){
		LookupParser p = new LookupParser(new LookupHandlerRegistryImpl());
		String query = "lookup lookup code AS in OUTPUT auth_code_desc as out";
		
		try {
			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("20701", e.getType());
			assertEquals(7, e.getStartOffset());
			assertEquals(12, e.getEndOffset());	
		}
	}
	
	@Test
	public void testError20702(){
		LookupParser p = new LookupParser(null);
		String query = "lookup sample code AS in OUTPUT auth_code_desc as out sample";
		
		try {
			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("20702", e.getType());
			assertEquals(32, e.getStartOffset());
			assertEquals(59, e.getEndOffset());	
		}
	}
	
	@Test
	public void testError20703(){
		LookupParser p = new LookupParser(null);
		String query = "lookup sample code AS in OUTPUT auth_code_desc ASa out ";
		
		try {
			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("20703", e.getType());
			assertEquals(32, e.getStartOffset());
			assertEquals(54, e.getEndOffset());	
		}
	}
	
}
