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

public class IndexOfTest {
	@Test
	public void testManual() {
		assertEquals(6, FunctionUtil.parseExpr("indexof(\"hello world\", \"world\")").eval(null));
		assertEquals(-1, FunctionUtil.parseExpr("indexof(\"hello world\", \"foo\")").eval(null));
		assertNull(FunctionUtil.parseExpr("indexof(\"hello world\", int(\"invalid\"))").eval(null));
		assertNull(FunctionUtil.parseExpr("indexof(int(\"invalid\"), \"world\")").eval(null));
		assertNull(FunctionUtil.parseExpr("indexof(int(\"invalid\"), int(\"invalid\"))").eval(null));
		assertEquals(7, FunctionUtil.parseExpr("indexof(\"hello world\", \"o\", 5)").eval(null));
	}
}
