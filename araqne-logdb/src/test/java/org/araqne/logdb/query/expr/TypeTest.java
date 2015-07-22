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

public class TypeTest {
	@Test
	public void testIntManual() {
		assertEquals(1234, FunctionUtil.parseExpr("int(\"1234\")").eval(null));
		assertEquals(1234, FunctionUtil.parseExpr("int(1234)").eval(null));

		        // There is no way to express null without using field yet.
		        //assertNull(FunctionUtil.parseExpr("int(null)").eval(null));

       assertNull(FunctionUtil.parseExpr("int(\"invalid\")").eval(null));
	}
	
	@Test
	public void testDoubleManual() {
		assertEquals(1.2, FunctionUtil.parseExpr("double(\"1.2\")").eval(null));
		assertEquals(0.0, FunctionUtil.parseExpr("double(\"0\")").eval(null));
		assertEquals(0.0, FunctionUtil.parseExpr("double(0)").eval(null));
		assertNull(FunctionUtil.parseExpr("double(\"invalid\")").eval(null));

	    // There is no way to express null without using field yet.
	    //assertNull(FunctionUtil.parseExpr("double(null)").eval(null));

	}
	
	@Test
	public void testLongManual() {
		assertEquals(1234L, FunctionUtil.parseExpr("long(\"1234\")").eval(null));
		assertEquals(1234L, FunctionUtil.parseExpr("long(1234)").eval(null));
		assertNull(FunctionUtil.parseExpr("long(\"invalid\")").eval(null));

	    // There is no way to express null without using field yet.
	    //assertNull(FunctionUtil.parseExpr("long(null)").eval(null));

	}
	
	@Test
	public void testStringManual() {
		assertEquals("1", FunctionUtil.parseExpr("string(1)").eval(null));
		assertEquals("1.2", FunctionUtil.parseExpr("string(1.2)").eval(null));
		assertEquals("true", FunctionUtil.parseExpr("string(true)").eval(null));
        assertNull(FunctionUtil.parseExpr("string(int(\"asdf\"))").eval(null));
		assertEquals("20140807164417", FunctionUtil.parseExpr("string(date(\"20140807164417\",\"yyyyMMddHHmmss\") ,\"yyyyMMddHHmmss\")").eval(null));
	}

	@Test
	public void testTypeofManual() {
		assertNull(FunctionUtil.parseExpr("typeof(int(\"asdf\"))").eval(null));
		assertEquals("string", FunctionUtil.parseExpr("typeof(\"sample\")").eval(null));
		assertEquals("int", FunctionUtil.parseExpr("typeof(1)").eval(null));
		assertEquals("long", FunctionUtil.parseExpr("typeof(2147483648)").eval(null));
		assertEquals("double", FunctionUtil.parseExpr("typeof(1.2)").eval(null));
		assertEquals("ipv4", FunctionUtil.parseExpr("typeof(ip(\"1.2.3.4\"))").eval(null));
		assertEquals("ipv6", FunctionUtil.parseExpr("typeof(ip(\"::1\"))").eval(null));
		assertEquals("bool", FunctionUtil.parseExpr("typeof(true)").eval(null));
	}
}
