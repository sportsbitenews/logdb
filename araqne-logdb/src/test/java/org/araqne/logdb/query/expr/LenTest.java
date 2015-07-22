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

public class LenTest {
	@Test
	public void testManual() {
		assertEquals(6, FunctionUtil.parseExpr("len(\"sample\")").eval(null));
		assertEquals(0, FunctionUtil.parseExpr("len(int(\"invalid\"))").eval(null));
		assertEquals(3, FunctionUtil.parseExpr("len(123)").eval(null));
		assertEquals(3, FunctionUtil.parseExpr("len(1.2)").eval(null));
	}

}
