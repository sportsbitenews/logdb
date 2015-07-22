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

public class MathFunctionTest {
	@Test
	public void testAbsManual() {
		assertEquals(1, FunctionUtil.parseExpr("abs(-1)").eval(null));
		assertEquals(1, FunctionUtil.parseExpr("abs(1)").eval(null));
		assertEquals(1.234, FunctionUtil.parseExpr("abs(-1.234)").eval(null));
		assertEquals(42L, FunctionUtil.parseExpr("abs(1 – 43)").eval(null));
	}

	@Test
	public void testCeilManual() {
		assertEquals(2L, FunctionUtil.parseExpr("ceil(1.1)").eval(null));
		assertEquals(2L, FunctionUtil.parseExpr("ceil(1.6)").eval(null));
		assertEquals(1.7, FunctionUtil.parseExpr("ceil(1.61, 1)").eval(null));
		assertEquals(1L, FunctionUtil.parseExpr("ceil(1.0)").eval(null));
		assertEquals(5, FunctionUtil.parseExpr("ceil(5)").eval(null));
		assertEquals(300L, FunctionUtil.parseExpr("ceil(297.5, -2)").eval(null));
		assertNull(FunctionUtil.parseExpr("ceil(\"asdf\")").eval(null));
		assertNull(FunctionUtil.parseExpr("ceil(\"1.1\")").eval(null));
		assertNull(FunctionUtil.parseExpr("ceil(1.1, \"eediom\")").eval(null));
	}
	
	@Test
	public void testFloorManual() {
		assertEquals(1L, FunctionUtil.parseExpr("floor(1.1)").eval(null));
		assertEquals(1L, FunctionUtil.parseExpr("floor(1.6)").eval(null));
		assertEquals(1.6, FunctionUtil.parseExpr("floor(1.61, 1)").eval(null));
		assertEquals(530L, FunctionUtil.parseExpr("floor(531, -1)").eval(null));
		assertEquals(1L, FunctionUtil.parseExpr("floor(1.0)").eval(null));
		assertEquals(5, FunctionUtil.parseExpr("floor(5)").eval(null));
		assertNull(FunctionUtil.parseExpr("floor(\"asdf\")").eval(null));
		assertNull(FunctionUtil.parseExpr("floor(\"1.1\")").eval(null));
		assertNull(FunctionUtil.parseExpr("floor(4.3, \"eediom\")").eval(null));
	}
	
	@Test
	public void testMaxManual() {
		assertEquals(1, FunctionUtil.parseExpr("max(1)").eval(null));
		assertEquals(2, FunctionUtil.parseExpr("max(1, 2)").eval(null));
		assertEquals(2, FunctionUtil.parseExpr("max(1, 2, int(\"invalid\"))").eval(null));
		assertNull(FunctionUtil.parseExpr("max(int(\"invalid\"))").eval(null));
	}
	
	@Test
	public void testMinManual() {
		assertEquals(1, FunctionUtil.parseExpr("min(1)").eval(null));
		assertEquals(1, FunctionUtil.parseExpr("min(1, 2)").eval(null));
		assertEquals(1, FunctionUtil.parseExpr("min(1, 2, int(\"invalid\"))").eval(null));
		assertNull(FunctionUtil.parseExpr("min(int(\"invalid\"))").eval(null));
	}

	@Test
	public void testRoundManual() {
		assertEquals(1L, FunctionUtil.parseExpr("round(1.1)").eval(null));
		assertEquals(2L, FunctionUtil.parseExpr("round(1.6)").eval(null));
		assertEquals(1L, FunctionUtil.parseExpr("round(1.0)").eval(null));
		assertEquals(1.5, FunctionUtil.parseExpr("round(1.47, 1)").eval(null));
		assertEquals(1800L, FunctionUtil.parseExpr("round(1837, -2)").eval(null));
		assertEquals(5, FunctionUtil.parseExpr("round(5)").eval(null));
		assertNull(FunctionUtil.parseExpr("round(\"asdf\")").eval(null));
		assertNull(FunctionUtil.parseExpr("round(\"1.1\")").eval(null));
		assertNull(FunctionUtil.parseExpr("round(37, 1.1)").eval(null));
	}
}
