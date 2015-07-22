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

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class ArrayTest {
	@Test
	public void testManual() {
		// There is no way to express null without using field yet.
		// assertArrayEquals(new Object[] {null}, (Object[])FunctionUtil.parseExpr("array(null)").eval(null));
		assertListEquals(Arrays.asList(new Object[] { 1 }), toList(FunctionUtil.parseExpr("array(1)").eval(null)));
		assertListEquals(Arrays.asList(new Object[] { "hello", "world" }), 
				toList(FunctionUtil.parseExpr("array(\"hello\", \"world\")").eval(null)));
		assertListEquals(Arrays.asList(new Object[] { 42L, "the answer to life, the universe, and everything" }),
				toList(FunctionUtil.parseExpr("array(21 * 2, \"the answer to life, the universe, and everything\")").eval(null)));
	}

	@SuppressWarnings("unchecked")
	static List<Object> toList(Object t) {
		return (List<Object>)t;
	}
	
	static void assertListEquals(List<Object> expected, List<Object> actual) {
		if (expected == null) {
			assertNull(actual);
		} else {
			assertNotNull(actual);
		}
		
		assertEquals(expected.size(), actual.size());
		
		for (int i = 0, n = expected.size(); i < n; ++i) {
			assertEquals(expected.get(i), actual.get(i));
		}
	}
}
