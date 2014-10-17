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

public class SubstrTest {
	@Test
	public void testManual() {
		assertEquals("012", FunctionUtil.parseExpr("substr(\"0123456789\", 0, 3)").eval(null));
		assertEquals("23", FunctionUtil.parseExpr("substr(\"0123456789\", 2, 4)").eval(null));
		assertEquals("456789", FunctionUtil.parseExpr("substr(\"0123456789\", 4, 12)").eval(null));
		assertEquals("", FunctionUtil.parseExpr("substr(\"0123456789\", 5, 5)").eval(null));
		assertNull(FunctionUtil.parseExpr("substr(\"0123456789\", 10, 11)").eval(null));
		assertNull(FunctionUtil.parseExpr("substr(int(\"asdf\"), 0, 3)").eval(null));
	}
}
