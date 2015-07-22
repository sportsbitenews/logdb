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

import java.util.Arrays;
import java.util.List;

import org.araqne.logdb.Row;
import org.junit.Test;

import static org.junit.Assert.*;

public class FlattenTest {
	@Test
	public void testNull() {
		Object o = getResult(null);
		assertNull(o);
	}

	@Test
	public void testScalar() {
		Object o = getResult(3L);
		assertEquals(3L, o);
	}

	@Test
	public void testList() {
		@SuppressWarnings("unchecked")
		List<Object> l = (List<Object>) getResult(Arrays.asList(1, 2, "foo"));
		assertEquals(3, l.size());
		assertEquals(1, l.get(0));
		assertEquals(2, l.get(1));
		assertEquals("foo", l.get(2));
	}

	@Test
	public void testNestedList() {
		@SuppressWarnings("unchecked")
		List<Object> l = (List<Object>) getResult(Arrays.asList(1, Arrays.asList(2, "foo"),
				Arrays.asList("foo", Arrays.asList("bar"), "baz")));

		assertEquals(6, l.size());
		assertEquals(1, l.get(0));
		assertEquals(2, l.get(1));
		assertEquals("foo", l.get(2));
		assertEquals("foo", l.get(3));
		assertEquals("bar", l.get(4));
		assertEquals("baz", l.get(5));
	}

	private Object getResult(Object var) {
		Row row = new Row();
		row.put("var", var);

		Expression expr = new EvalField("var");
		Flatten f = new Flatten(null, Arrays.asList(expr));
		Object o = f.eval(row);
		return o;
	}
}
