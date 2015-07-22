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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.Term;
import org.araqne.logdb.query.command.Term.Operator;
import org.junit.Test;

public class TermParserTest {
	@Test
	public void testEqual() {
		final String TERM = "field1 == field2";

		ParseResult r = TermParser.parseTerm(TERM);
		Term term = (Term) r.value;

		assertNotNull(term);
		assertEquals("field1", term.getLh());
		assertEquals("field2", term.getRh());
		assertEquals(Operator.Eq, term.getOperator());
	}

	@Test
	public void testEqualValue() {
		final String TERM = "field1 == \"value\"";

		ParseResult r = TermParser.parseTerm(TERM);
		Term term = (Term) r.value;

		assertNotNull(term);
		assertEquals("field1", term.getLh());
		assertEquals("value", term.getRh());
		assertTrue(term.isRhString());
	}

	@Test
	public void testEqualBrokenQuotedValue() {
		final String TERM = "field1 == \"value";

		try {
			TermParser.parseTerm(TERM);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + TERM);
				System.out.println(e.getMessage());
			}
			assertEquals("90005", e.getType());
			assertEquals(TERM, e.getParams().get("value"));
		} 
	}

	@Test
	public void testBrokenEqual() {
		final String TERM = "field1 ==";

		try {
			TermParser.parseTerm(TERM);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + TERM);
				System.out.println(e.getMessage());
			}
			assertEquals("90004", e.getType());
			assertEquals(TERM, e.getParams().get("value"));
		} 
//		catch (QueryParseException e) {
//			assertEquals("need-string-token", e.getType());
//			assertEquals(9, (int) e.getOffset());
//		}
	}

	@Test
	public void testNotEqual() {
		final String TERM = "field1 != field2";

		ParseResult r = TermParser.parseTerm(TERM);
		Term term = (Term) r.value;

		assertNotNull(term);
		assertEquals("field1", term.getLh());
		assertEquals("field2", term.getRh());
		assertEquals(Operator.Neq, term.getOperator());
	}

	@Test
	public void testGt() {
		final String TERM = "field1 > field2";

		ParseResult r = TermParser.parseTerm(TERM);
		Term term = (Term) r.value;

		assertNotNull(term);
		assertEquals("field1", term.getLh());
		assertEquals("field2", term.getRh());
		assertEquals(Operator.Gt, term.getOperator());
	}

	@Test
	public void testGte() {
		final String TERM = "field1 >= field2";

		ParseResult r = TermParser.parseTerm(TERM);
		Term term = (Term) r.value;

		assertNotNull(term);
		assertEquals("field1", term.getLh());
		assertEquals("field2", term.getRh());
		assertEquals(Operator.Ge, term.getOperator());
	}

	@Test
	public void testLt() {
		final String TERM = "field1 < field2";

		ParseResult r = TermParser.parseTerm(TERM);
		Term term = (Term) r.value;

		assertNotNull(term);
		assertEquals("field1", term.getLh());
		assertEquals("field2", term.getRh());
		assertEquals(Operator.Lt, term.getOperator());
	}

	@Test
	public void testLte() {
		final String TERM = "field1 <= field2";

		ParseResult r = TermParser.parseTerm(TERM);
		Term term = (Term) r.value;

		assertNotNull(term);
		assertEquals("field1", term.getLh());
		assertEquals("field2", term.getRh());
		assertEquals(Operator.Le, term.getOperator());
	}

	@Test
	public void testContain() {
		final String TERM = "field1 contain \"needle\"";

		ParseResult r = TermParser.parseTerm(TERM);
		Term term = (Term) r.value;

		assertNotNull(term);
		assertEquals("field1", term.getLh());
		assertEquals("needle", term.getRh());
		assertTrue(term.isRhString());
		assertEquals(Operator.Contain, term.getOperator());
	}

	@Test
	public void testRegexp() {
		final String TERM = "field1 regexp \"\\d+\"";

		ParseResult r = TermParser.parseTerm(TERM);
		Term term = (Term) r.value;

		assertNotNull(term);
		assertEquals("field1", term.getLh());
		assertEquals("\\d+", term.getRh());
		assertTrue(term.isRhString());
		assertEquals(Operator.Regexp, term.getOperator());
	}

	@Test
	public void testIn() {
		final String TERM = "field1 in (a, b, c)";

		ParseResult r = TermParser.parseTerm(TERM);
		Term term = (Term) r.value;

		assertNotNull(term);
		assertEquals("field1", term.getLh());
		assertEquals("(a, b, c)", term.getRh());
		assertFalse(term.isRhString());
		assertEquals(Operator.In, term.getOperator());
	}

	@Test
	public void testIsNull() {
		final String TERM = "field1 is null";

		ParseResult r = TermParser.parseTerm(TERM);
		Term term = (Term) r.value;

		assertNotNull(term);
		assertEquals("field1", term.getLh());
		assertNull(term.getRh());
		assertEquals(Operator.IsNull, term.getOperator());
	}

	@Test
	public void testNotNull() {
		final String TERM = "field1 not null";

		ParseResult r = TermParser.parseTerm(TERM);
		Term term = (Term) r.value;

		assertNotNull(term);
		assertEquals("field1", term.getLh());
		assertNull(term.getRh());
		assertEquals(Operator.NotNull, term.getOperator());
	}
	
	@Test
	public void testUnsupportedOp() {
		final String TERM = "field1 <> field2";

		try {
			TermParser.parseTerm(TERM);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println(TERM);
				System.out.println(e.getMessage());
			}
			assertEquals("21900", e.getType());
			assertEquals(TERM, e.getParams().get("value"));
			assertEquals("<>", e.getParams().get("op"));
		} 
	}
}
