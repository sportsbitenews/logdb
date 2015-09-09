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

import java.util.Iterator;

import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.Rename;
import org.junit.Test;

public class RenameParserTest {
	@Test
	public void testParse() {
		RenameParser p = new RenameParser();
		Rename rename = (Rename) p.parse(null, "rename sent as Sent");
		Rename.Pair pair = rename.getPairs().iterator().next();
		assertEquals("sent", pair.getFrom());
		assertEquals("Sent", pair.getTo());

		assertEquals("rename sent as Sent", rename.toString());
	}

	@Test
	public void testMulti() {
		RenameParser p = new RenameParser();
		Rename rename = (Rename) p.parse(null, "rename sent as Sent,  boy as girl, people as    team  ");
		Iterator<Rename.Pair> it = rename.getPairs().iterator();
		{
			Rename.Pair pair = it.next();
			assertEquals("sent", pair.getFrom());
			assertEquals("Sent", pair.getTo());
		}
		{
			Rename.Pair pair = it.next();
			assertEquals("boy", pair.getFrom());
			assertEquals("girl", pair.getTo());
		}
		{
			Rename.Pair pair = it.next();
			assertEquals("people", pair.getFrom());
			assertEquals("team", pair.getTo());
		}

		assertEquals("rename sent as Sent, boy as girl, people as team", rename.toString());
	}
	
	@Test
	public void testMultiBrokenCase1() {
		RenameParser p = new RenameParser();
		String query = "rename sent as Sent,  , people as team";
		try {
			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if (e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("20800", e.getType());
			assertEquals(21, e.getStartOffset());
			assertEquals(21, e.getEndOffset());	
		}
	}

	@Test
	public void testMultiBrokenCase2() {
		RenameParser p = new RenameParser();
		String query = "rename sent as Sent, boy , people as team";
		try {
			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if (e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("20801", e.getType());
			assertEquals(21, e.getStartOffset());
			assertEquals(24, e.getEndOffset());	
		}
	}

	@Test
	public void testMultiBrokenCase3() {
		RenameParser p = new RenameParser();
		String query = "rename sent as Sent, boy of girl, people as team";
		try {
			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if (e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("20802", e.getType());
			assertEquals(25, e.getStartOffset());
			assertEquals(26, e.getEndOffset());	
		}
	}

	@Test
	public void testBrokenCase1() {
		RenameParser p = new RenameParser();
		String query = "rename  ";
		
		try {
			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("20800", e.getType());
			assertEquals(7, e.getStartOffset());
			assertEquals(7, e.getEndOffset());	
		}
	}

	@Test
	public void testBrokenCase2() {
		RenameParser p = new RenameParser();
		String query = "rename   sent";
		
		try {
			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if (e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("20801", e.getType());
			assertEquals(9, e.getStartOffset());
			assertEquals(12, e.getEndOffset());	
		}
		
		query = "rename sent   as";
		try {
			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("20801", e.getType());
			assertEquals(7, e.getStartOffset());
			assertEquals(15, e.getEndOffset());	
		}
	}
	
	@Test
	public void testBrokenCase3() {
		RenameParser p = new RenameParser();
		String query = "rename sent  sent Sent";
		
		try {
			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("20802", e.getType());
			assertEquals(13, e.getStartOffset());
			assertEquals(16, e.getEndOffset());	
		}
	}
}
