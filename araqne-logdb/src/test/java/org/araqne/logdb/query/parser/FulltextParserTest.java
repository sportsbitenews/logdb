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

import org.junit.Test;
import static org.junit.Assert.*;

import org.araqne.logdb.LogQueryParseException;
import org.araqne.logdb.query.command.Fulltext;
import org.araqne.logstorage.LogIndexQuery;

public class FulltextParserTest {
	@Test
	public void testSimpleIndexQuery() {
		FulltextParser p = new FulltextParser(null, null);
		Fulltext fulltext = (Fulltext) p.parse(null, "fulltext 1.2.3.4");
		LogIndexQuery query = fulltext.getQuery();
		assertEquals("1.2.3.4", query.getTerm());
	}

	@Test
	public void testQualifiers() {
		FulltextParser p = new FulltextParser(null, null);
		Fulltext fulltext = (Fulltext) p.parse(null, "fulltext table=test index=ip 1.2.3.4");
		LogIndexQuery query = fulltext.getQuery();
		assertEquals("1.2.3.4", query.getTerm());
		assertEquals("test", query.getTableName());
		assertEquals("ip", query.getIndexName());

	}

	@Test
	public void testMissingArgs() {
		FulltextParser p = new FulltextParser(null, null);
		try {
			p.parse(null, "fulltext table=test ");
			fail();
		} catch (LogQueryParseException e) {
			assertEquals("term-not-found", e.getType());
		}
	}

}
