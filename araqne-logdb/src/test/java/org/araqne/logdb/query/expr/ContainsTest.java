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

import org.junit.Test;

public class ContainsTest {
	@Test
	public void testManual() {
		assertEquals(true, FunctionUtil.parseExpr("contains(\"foo\", \"o\")").eval(null));
		assertEquals(false, FunctionUtil.parseExpr("contains(\"bar\", \"o\")").eval(null));
		assertEquals(false, FunctionUtil.parseExpr("contains(\"baz\", int(\"invalid\"))").eval(null));
		assertEquals(false, FunctionUtil.parseExpr("contains(int(\"invalid\"), int(\"invalid\"))").eval(null));
	}

	@Test
	public void testArray() {
		assertTrue((Boolean) FunctionUtil.parseExpr("contains(\"stabcnia\", \"abc\"").eval(null));
		assertTrue((Boolean) FunctionUtil.parseExpr("contains(array(\"1\",\"abc\",\"2\"), \"abc\"").eval(null));
		assertFalse((Boolean) FunctionUtil.parseExpr("contains(array(\"1\",\"abc\",\"2\"), array(\"abc\",\"2\"))").eval(null));
		assertTrue((Boolean) FunctionUtil.parseExpr("contains(array(\"1\",array(\"abc\",\"2\"),\"2\"), array(\"abc\",\"2\"))")
				.eval(null));
	}
}
