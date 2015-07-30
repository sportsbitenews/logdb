/**
 * Copyright 2015 Eediom Inc.
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

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.Test;

/**
 * 
 * @author kyun
 * 
 */
public class ToIpTest {

	@Test
	public void testNormal() throws UnknownHostException {
		// int
		assertEquals(InetAddress.getByName("127.0.0.1"), FunctionUtil.parseExpr("ip(2130706433)").eval(null));
		assertEquals(InetAddress.getByName("192.168.0.1"), FunctionUtil.parseExpr("ip(-1062731775)").eval(null));
		assertEquals(InetAddress.getByName("255.255.255.255"), FunctionUtil.parseExpr("ip(-1)").eval(null));
		assertEquals(InetAddress.getByName("1.2.3.4"), FunctionUtil.parseExpr("ip(16909060)").eval(null));
		
		// long
		assertEquals(InetAddress.getByName("192.168.0.1"), FunctionUtil.parseExpr("ip(3232235521)").eval(null));
		assertEquals(InetAddress.getByName("255.255.255.255"), FunctionUtil.parseExpr("ip(4294967295)").eval(null));
		
		// string
		assertEquals(InetAddress.getByName("127.0.0.1"), FunctionUtil.parseExpr("ip(\"127.0.0.1\")").eval(null));
		assertEquals(InetAddress.getByName("192.168.0.1"), FunctionUtil.parseExpr("ip(\"192.168.0.1\")").eval(null));
		assertEquals(InetAddress.getByName("255.255.255.255"), FunctionUtil.parseExpr("ip(\"255.255.255.255\")").eval(null));

		// invalid
		assertNull(FunctionUtil.parseExpr("ip(\"-0.0.0.0\")").eval(null));
		assertNull(FunctionUtil.parseExpr("ip(\"0.0.0,0\")").eval(null));

		assertNull(FunctionUtil.parseExpr("ip(\"256.0.0.1\")").eval(null));
		assertNull(FunctionUtil.parseExpr("ip(\"2222.0.0.0\")").eval(null));
		assertNull(FunctionUtil.parseExpr("ip(\"22222.11111.0.0\")").eval(null));

		assertNull(FunctionUtil.parseExpr("ip(\"1.2.3.4.5\")").eval(null));
		assertNull(FunctionUtil.parseExpr("ip(\"1.2.3.4.5.6\")").eval(null));
		
	}

	
	
	
	
	

}
