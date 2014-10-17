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

public class LeftTest {
	@Test
	public void testManual() {
		assertEquals("0123", FunctionUtil.parseExpr("left(\"0123456789\", 4)").eval(null));
		assertEquals("0123456789", FunctionUtil.parseExpr("left(\"0123456789\", 11)").eval(null));
		assertEquals("", FunctionUtil.parseExpr("left(\"0123456789\", 0)").eval(null));
		assertEquals("12", FunctionUtil.parseExpr("left(1234, 2)").eval(null));
		assertEquals("1.2", FunctionUtil.parseExpr("left(1.23, 3)").eval(null));
		assertNull(FunctionUtil.parseExpr("left(int(\"invalid\"), 3)").eval(null));
	}

}
