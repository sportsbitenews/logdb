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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.araqne.logdb.QueryParseException;
import org.junit.Test;

/**
 * 
 * @author kyun
 * 
 */
public class ToIntTest {

	@Test
	public void testNormal() {
		// null
		assertNull(FunctionUtil.parseExpr("int(null)").eval(null));

		// int
		assertEquals(10, FunctionUtil.parseExpr("int(10)").eval(null));
		assertEquals(0, FunctionUtil.parseExpr("int(0)").eval(null));
		assertEquals(-15210, FunctionUtil.parseExpr("int(-15210)").eval(null));
		assertNull(FunctionUtil.parseExpr("int(11111111111)").eval(null));

		// double
		assertEquals(10, FunctionUtil.parseExpr("int(10.5)").eval(null));
		assertEquals(0, FunctionUtil.parseExpr("int(0.3)").eval(null));
		assertEquals(-1, FunctionUtil.parseExpr("int(-1.2)").eval(null));
		assertEquals(-11111111, FunctionUtil.parseExpr("int(-11111111.2)").eval(null));
		assertNull(FunctionUtil.parseExpr("int(11111111111.2)").eval(null));

		// String
		assertEquals(10, FunctionUtil.parseExpr("int(\"10\")").eval(null));
		assertEquals(0, FunctionUtil.parseExpr("int(\"0\")").eval(null));
		assertEquals(-15210, FunctionUtil.parseExpr("int(\"-15210\")").eval(null));
		assertNull(FunctionUtil.parseExpr("int(\"11111111111\")").eval(null));

		// invalid String
		assertNull(FunctionUtil.parseExpr("int(\"1.0\")").eval(null));
	}

	@Test
	public void testIP() {
		// ip
		assertEquals(16909060, FunctionUtil.parseExpr("int(ip(\"1.2.3.4\"))").eval(null));
		assertEquals(-1062731775, FunctionUtil.parseExpr("int(ip(\"192.168.0.1\"))").eval(null));

		// corner case
		assertEquals(0, FunctionUtil.parseExpr("int(ip(\"0.0.0.0\"))").eval(null));
		assertEquals(-1, FunctionUtil.parseExpr("int(ip(\"255.255.255.255\"))").eval(null));

		// invalid String
		assertNull(FunctionUtil.parseExpr("int(\"1.2.3.4\")").eval(null));
		assertNull(FunctionUtil.parseExpr("int(\"0.0.0.0\")").eval(null));
		assertNull(FunctionUtil.parseExpr("int(\"255.255.255.255\")").eval(null));

		// invalid chars
		assertNull(FunctionUtil.parseExpr("int(ip(\"-0.0.0.0\"))").eval(null));
		assertNull(FunctionUtil.parseExpr("int(ip(\"0.0.0,0\"))").eval(null));

		// invalid range
		assertNull(FunctionUtil.parseExpr("int(ip(\"256.0.0.1\"))").eval(null));
		assertNull(FunctionUtil.parseExpr("int(ip(\"2222.0.0.0\"))").eval(null));
		assertNull(FunctionUtil.parseExpr("int(ip(\"22222.11111.0.0\"))").eval(null));

		// invalid number count
		assertNull(FunctionUtil.parseExpr("int(ip(\"1.2.3.4.5\"))").eval(null));
		assertNull(FunctionUtil.parseExpr("int(ip(\"1.2.3.4.5.6\"))").eval(null));
	}

	@Test
	public void testError90830() {
		try {
			assertNull(FunctionUtil.parseExpr("int(10, 5)").eval(null));
			fail();
		} catch (QueryParseException e) {
			if (e.isDebugMode()) {
				System.out.println(e.getMessage());
			}
			assertEquals("90830", e.getType());
		}
	}
}
