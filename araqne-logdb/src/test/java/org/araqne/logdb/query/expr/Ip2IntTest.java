package org.araqne.logdb.query.expr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class Ip2IntTest {
	@Test
	public void testIp2Int() {
		// normal case
		assertEquals(16909060, FunctionUtil.parseExpr("ip2int(\"1.2.3.4\")").eval(null)); 
		assertEquals(16909060, FunctionUtil.parseExpr("ip2int(ip(\"1.2.3.4\"))").eval(null)); 
		assertEquals(-1062731775, FunctionUtil.parseExpr("ip2int(\"192.168.0.1\")").eval(null)); 
		assertEquals(-1062731775, FunctionUtil.parseExpr("ip2int(ip(\"192.168.0.1\"))").eval(null)); 

		// corner case
		assertEquals(0,  FunctionUtil.parseExpr("ip2int(\"0.0.0.0\")").eval(null));
		assertEquals(-1, FunctionUtil.parseExpr("ip2int(\"255.255.255.255\")").eval(null));
		assertEquals(0,  FunctionUtil.parseExpr("ip2int(ip(\"0.0.0.0\"))").eval(null));
		assertEquals(-1, FunctionUtil.parseExpr("ip2int(ip(\"255.255.255.255\"))").eval(null));

		// invalid chars
		assertNull(FunctionUtil.parseExpr("ip2int(\"-0.0.0.0\")").eval(null));
		assertNull(FunctionUtil.parseExpr("ip2int(\"0.0.0,0\")").eval(null));
		assertNull(FunctionUtil.parseExpr("ip2int(ip(\"-0.0.0.0\"))").eval(null));
		assertNull(FunctionUtil.parseExpr("ip2int(ip(\"0.0.0,0\"))").eval(null));

		// invalid range
		assertNull(FunctionUtil.parseExpr("ip2int(\"256.0.0.1\")").eval(null));
		assertNull(FunctionUtil.parseExpr("ip2int(\"2222.0.0.0\")").eval(null));
		assertNull(FunctionUtil.parseExpr("ip2int(\"22222.11111.0.0\")").eval(null));
		assertNull(FunctionUtil.parseExpr("ip2int(ip(\"256.0.0.1\"))").eval(null));
		assertNull(FunctionUtil.parseExpr("ip2int(ip(\"2222.0.0.0\"))").eval(null));
		assertNull(FunctionUtil.parseExpr("ip2int(ip(\"22222.11111.0.0\"))").eval(null));

		// invalid number count
		assertNull(FunctionUtil.parseExpr("ip2int(\"1.2.3.4.5\")").eval(null));
		assertNull(FunctionUtil.parseExpr("ip2int(\"1.2.3.4.5.6\")").eval(null));
		assertNull(FunctionUtil.parseExpr("ip2int(ip(\"1.2.3.4.5\"))").eval(null));
		assertNull(FunctionUtil.parseExpr("ip2int(ip(\"1.2.3.4.5.6\"))").eval(null));
	}
}
