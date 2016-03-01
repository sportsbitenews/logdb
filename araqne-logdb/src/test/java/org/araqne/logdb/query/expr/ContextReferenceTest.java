/**
 * Copyright 2014 Eediom Inc.
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
package org.araqne.logdb.query.expr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;
import org.junit.Test;

/**
 * 
 * @author kyun
 *
 */
public class ContextReferenceTest {

	@Test
	public void testContextReference() {
		ContextReference r = $("EDM");
		assertEquals(r.eval((Row) null), "eediom");
	}

	@Test
	public void testContextReferenceNull() {
		try {
			$(null);
			fail();
		} catch (QueryParseException e) {
			if (e.isDebugMode()) {
				System.out.println(e.getMessage());
			}
			assertEquals("90611", e.getType());
		}
	}

	@Test
	public void testContextReferenceNull2() {
		try {
			$();
			fail();
		} catch (QueryParseException e) {
			if (e.isDebugMode()) {
				System.out.println(e.getMessage());
			}
			assertEquals("90610", e.getType());
		}
	}

	private ContextReference $() {
		return new ContextReference(QueryContext(), new ArrayList<Expression>());
	}

	private ContextReference $(String s) {
		return new ContextReference(QueryContext(), expr(s));
	}

	private List<Expression> expr(String... number) {
		List<Expression> expr = new ArrayList<Expression>();

		for (String n : number)
			expr.add(new StringConstant(n));

		return expr;
	}

	private QueryContext QueryContext() {
		QueryContext mockContextReferenceRegistry = mock(QueryContext.class);
		Map<String, Object> constants = new ConcurrentHashMap<String, Object>();
		constants.put("EDM", "eediom");
		when(mockContextReferenceRegistry.getConstants()).thenReturn(constants);

		return mockContextReferenceRegistry;
	}

}
