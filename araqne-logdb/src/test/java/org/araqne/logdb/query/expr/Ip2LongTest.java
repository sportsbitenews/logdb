package org.araqne.logdb.query.expr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class Ip2LongTest {
	@Test
	public void testIp2Long() {
		// normal case
		assertEquals(16909060L, FunctionUtil.parseExpr("ip2long(\"1.2.3.4\")").eval(null)); 
		assertEquals(16909060L, FunctionUtil.parseExpr("ip2long(ip(\"1.2.3.4\"))").eval(null)); 

		
		// corner case
		assertEquals(0L,  FunctionUtil.parseExpr("ip2long(\"0.0.0.0\")").eval(null));
		assertEquals(4294967295L, FunctionUtil.parseExpr("ip2long(\"255.255.255.255\")").eval(null));
		assertEquals(0L,  FunctionUtil.parseExpr("ip2long(ip(\"0.0.0.0\"))").eval(null));
		assertEquals(4294967295L, FunctionUtil.parseExpr("ip2long(ip(\"255.255.255.255\"))").eval(null));

		// invalid chars
		assertNull(FunctionUtil.parseExpr("ip2long(\"-0.0.0.0\")").eval(null));
		assertNull(FunctionUtil.parseExpr("ip2long(\"0.0.0,0\")").eval(null));
		assertNull(FunctionUtil.parseExpr("ip2long(ip(\"-0.0.0.0\"))").eval(null));
		assertNull(FunctionUtil.parseExpr("ip2long(ip(\"0.0.0,0\"))").eval(null));

		// invalid range
		assertNull(FunctionUtil.parseExpr("ip2long(\"256.0.0.1\")").eval(null));
		assertNull(FunctionUtil.parseExpr("ip2long(\"2222.0.0.0\")").eval(null));
		assertNull(FunctionUtil.parseExpr("ip2long(\"22222.11111.0.0\")").eval(null));
		assertNull(FunctionUtil.parseExpr("ip2long(ip(\"256.0.0.1\"))").eval(null));
		assertNull(FunctionUtil.parseExpr("ip2long(ip(\"2222.0.0.0\"))").eval(null));
		assertNull(FunctionUtil.parseExpr("ip2long(ip(\"22222.11111.0.0\"))").eval(null));

		// invalid number count
		assertNull(FunctionUtil.parseExpr("ip2long(\"1.2.3.4.5\")").eval(null));
		assertNull(FunctionUtil.parseExpr("ip2long(\"1.2.3.4.5.6\")").eval(null));
		assertNull(FunctionUtil.parseExpr("ip2long(ip(\"1.2.3.4.5\"))").eval(null));
		assertNull(FunctionUtil.parseExpr("ip2long(ip(\"1.2.3.4.5.6\"))").eval(null));
	}
}
