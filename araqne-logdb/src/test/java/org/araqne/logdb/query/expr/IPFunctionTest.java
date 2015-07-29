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

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.Test;

public class IPFunctionTest {
	// TODO : ip
	@Test
	public void testIP2LongManual() {
		assertEquals(3232235521L, FunctionUtil.parseExpr("ip2long(\"192.168.0.1\")").eval(null));
		assertEquals(2130706433L, FunctionUtil.parseExpr("ip2long(\"127.0.0.1\")").eval(null));

		assertEquals(3232235521L, FunctionUtil.parseExpr("ip2long(ip(\"192.168.0.1\"))").eval(null));
		assertEquals(2130706433L, FunctionUtil.parseExpr("ip2long(ip(\"127.0.0.1\"))").eval(null));
	}

	@Test
	public void testIP2IntManual() {
		assertEquals(-1062731775, FunctionUtil.parseExpr("ip2int(\"192.168.0.1\")").eval(null));
		assertEquals(2130706433, FunctionUtil.parseExpr("ip2int(\"127.0.0.1\")").eval(null));

		assertEquals(2130706433, FunctionUtil.parseExpr("ip2int(ip(\"127.0.0.1\"))").eval(null));
		assertEquals(-1062731775, FunctionUtil.parseExpr("ip2int(ip(\"192.168.0.1\"))").eval(null));
	}

	@Test
	public void testLong2IPManual() {
		assertEquals("192.168.0.1", FunctionUtil.parseExpr("long2ip(3232235521)").eval(null));
		assertEquals("192.168.0.1", FunctionUtil.parseExpr("long2ip(-1062731775)").eval(null));
		assertEquals("127.0.0.1", FunctionUtil.parseExpr("long2ip(2130706433)").eval(null));
	}

	@Test
	public void testLongManual() {
		assertEquals(3232235521L, FunctionUtil.parseExpr("long(ip(\"192.168.0.1\"))").eval(null));
		assertEquals(2130706433L, FunctionUtil.parseExpr("long(ip(\"127.0.0.1\"))").eval(null));
	}

	@Test
	public void testIntManual() {
		assertEquals(2130706433, FunctionUtil.parseExpr("int(ip(\"127.0.0.1\"))").eval(null));
		assertEquals(-1062731775, FunctionUtil.parseExpr("int(ip(\"192.168.0.1\"))").eval(null));
	}

	@Test
	public void testIpManual() throws UnknownHostException {
		assertEquals(InetAddress.getByName("127.0.0.1"), FunctionUtil.parseExpr("ip(2130706433)").eval(null));
		assertEquals(InetAddress.getByName("192.168.0.1"), FunctionUtil.parseExpr("ip(-1062731775)").eval(null));
		assertEquals(InetAddress.getByName("192.168.0.1"), FunctionUtil.parseExpr("ip(3232235521)").eval(null));
	}

	@Test
	public void testNetworkManual() {
		assertNull(FunctionUtil.parseExpr("network(int(\"invalid\"), 32)").eval(null));
		assertEquals("255.255.255.255", FunctionUtil.parseExpr("network(\"255.255.255.255\", 32)").eval(null));
		assertEquals("255.255.255.0", FunctionUtil.parseExpr("network(\"255.255.255.255\", 24)").eval(null));
		assertEquals("21da:d3:0:2f3b:2aa:ff:0:0",
				FunctionUtil.parseExpr("network(\"21DA:00D3:0000:2F3B:02AA:00FF:FE28:9C5A”, 96)").eval(null));
		assertEquals("0:0:0:0:0:0:0:0",
				FunctionUtil.parseExpr("network(\"21DA:00D3:0000:2F3B:02AA:00FF:FE28:9C5A”, 0)").eval(null));
	}
}
