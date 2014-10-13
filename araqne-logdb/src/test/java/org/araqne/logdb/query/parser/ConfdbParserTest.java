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

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Session;
import org.araqne.logdb.query.command.Confdb;
import org.junit.Test;

public class ConfdbParserTest {
	
	@Test
	public void testError10000(){
		String query = "confdb databases";
		ConfdbParser parse = new ConfdbParser(null);
		
		try {
			parse.parse(null,  query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("10000", e.getType());
			assertEquals(-1, e.getStartOffset());
			assertEquals(-1, e.getEndOffset());	
		}
	}

	@Test
	public void testError10001() {
		String query = "confdb";
		
		try {
			parse(query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("10001", e.getType());
			assertEquals(7, e.getStartOffset());
			assertEquals(5, e.getEndOffset());	
		}
	}
	
	@Test
	public void testError10002() {
		String query = "confdb cols";
		
		try {
			parse(query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("10002", e.getType());
			assertEquals(10, e.getStartOffset());
			assertEquals(10, e.getEndOffset());	
		}
	}
	
	@Test
	public void testError10003() {
		String query = "confdb docs araqne-log-api";
		
		try {
			parse(query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("10003", e.getType());
			assertEquals(25, e.getStartOffset());
			assertEquals(25, e.getEndOffset());	
		}
	}
	
	@Test
	public void testError10004() {
		String query = "confdb confdb";
		
		try {
			parse(query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("10004", e.getType());
			assertEquals(7, e.getStartOffset());
			assertEquals(12, e.getEndOffset());	
		}
	}
	
	
	
	private Confdb parse(String query){
		Session session = mock(Session.class);
		when(session.isAdmin()).thenReturn(true);
		QueryContext context = new QueryContext(session);
		ConfdbParser parse = new ConfdbParser(null);

		return (Confdb) parse.parse(context, query);
	}
	
}
