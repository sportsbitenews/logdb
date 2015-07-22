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

public class IsFunctionTest {
	@Test
	public void testIsNotNullManual() {
		assertTrue((Boolean)(FunctionUtil.parseExpr("isnotnull(1)")).eval(null));
		assertFalse((Boolean)(FunctionUtil.parseExpr("isnotnull(int(\"asdfas\"))").eval(null)));
	}

	@Test
	public void testIsNullManual() {
		assertFalse((Boolean)(FunctionUtil.parseExpr("isnull(1)")).eval(null));
		assertTrue((Boolean)(FunctionUtil.parseExpr("isnull(int(\"asdfas\"))").eval(null)));
	}
	
	@Test
	public void testIsNumManual() {
		assertTrue((Boolean)(FunctionUtil.parseExpr("isnum(1)")).eval(null));
		assertTrue((Boolean)(FunctionUtil.parseExpr("isnum(1.2)")).eval(null));
		assertFalse((Boolean)(FunctionUtil.parseExpr("isnum(\"string\")")).eval(null));
		assertFalse((Boolean)(FunctionUtil.parseExpr("isnum(int(\"asdfas\"))").eval(null)));
	}

	@Test
	public void testIsStrManual() {
		assertTrue((Boolean)(FunctionUtil.parseExpr("isstr(\"string\")")).eval(null));
		assertFalse((Boolean)(FunctionUtil.parseExpr("isstr(0)")).eval(null));
		assertFalse((Boolean)(FunctionUtil.parseExpr("isstr(int(\"asdfas\"))").eval(null)));
	}
}
