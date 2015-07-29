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
public class ToLongTest {

	@Test
	public void testNormal() {
		// long
		assertEquals(10L, FunctionUtil.parseExpr("long(10)").eval(null));
		assertEquals(0L, FunctionUtil.parseExpr("long(0)").eval(null));
		assertEquals(-15210L, FunctionUtil.parseExpr("long(-15210)").eval(null));
		assertEquals(11111111111L, FunctionUtil.parseExpr("long(11111111111)").eval(null));

		// double
		assertEquals(10L, FunctionUtil.parseExpr("long(10.5)").eval(null));
		assertEquals(0L, FunctionUtil.parseExpr("long(0.3)").eval(null));
		assertEquals(-1L, FunctionUtil.parseExpr("long(-1.2)").eval(null));
		assertEquals(11111111111L, FunctionUtil.parseExpr("long(11111111111.2)").eval(null));

		// String
		assertEquals(10L, FunctionUtil.parseExpr("long(\"10\")").eval(null));
		assertEquals(0L, FunctionUtil.parseExpr("long(\"0\")").eval(null));
		assertEquals(-15210L, FunctionUtil.parseExpr("long(\"-15210\")").eval(null));
		assertEquals(11111111111L, FunctionUtil.parseExpr("long(\"11111111111\")").eval(null));

		// invalid String
		assertNull(FunctionUtil.parseExpr("long(\"1.0\")").eval(null));
	}

	@Test
	public void testIP() {
		// ip
		assertEquals(16909060L, FunctionUtil.parseExpr("long(ip(\"1.2.3.4\"))").eval(null));
		assertEquals(3232235521L, FunctionUtil.parseExpr("long(ip(\"192.168.0.1\"))").eval(null));

		// corner case
		assertEquals(0L, FunctionUtil.parseExpr("long(ip(\"0.0.0.0\"))").eval(null));
		assertEquals(4294967295L, FunctionUtil.parseExpr("long(ip(\"255.255.255.255\"))").eval(null));

		// invalid String
		assertNull(FunctionUtil.parseExpr("long(\"1.2.3.4\")").eval(null));
		assertNull(FunctionUtil.parseExpr("long(\"0.0.0.0\")").eval(null));
		assertNull(FunctionUtil.parseExpr("long(\"255.255.255.255\")").eval(null));

		// invalid chars
		assertNull(FunctionUtil.parseExpr("long(ip(\"-0.0.0.0\"))").eval(null));
		assertNull(FunctionUtil.parseExpr("long(ip(\"0.0.0,0\"))").eval(null));

		// invalid range
		assertNull(FunctionUtil.parseExpr("long(ip(\"256.0.0.1\"))").eval(null));
		assertNull(FunctionUtil.parseExpr("long(ip(\"2222.0.0.0\"))").eval(null));
		assertNull(FunctionUtil.parseExpr("long(ip(\"22222.11111.0.0\"))").eval(null));

		// invalid number count
		assertNull(FunctionUtil.parseExpr("long(ip(\"1.2.3.4.5\"))").eval(null));
		assertNull(FunctionUtil.parseExpr("long(ip(\"1.2.3.4.5.6\"))").eval(null));
	}

	@Test
	public void testError90840() {
		try {
			assertNull(FunctionUtil.parseExpr("long(10, 5)").eval(null));
			fail();
		} catch (QueryParseException e) {
			if (e.isDebugMode()) {
				System.out.println(e.getMessage());
			}
			assertEquals("90840", e.getType());
		}
	}

}
