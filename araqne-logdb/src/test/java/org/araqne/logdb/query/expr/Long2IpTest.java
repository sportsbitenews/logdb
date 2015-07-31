package org.araqne.logdb.query.expr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class Long2IpTest {
	@Test
	public void testLong2Ip() {

		// normal case
		assertEquals("192.168.0.1", FunctionUtil.parseExpr("long2ip(3232235521)").eval(null));
		assertEquals("192.168.0.1", FunctionUtil.parseExpr("long2ip(-1062731775)").eval(null));
		assertEquals("127.0.0.1", FunctionUtil.parseExpr("long2ip(2130706433)").eval(null));

		// int
		assertEquals("192.168.0.1", FunctionUtil.parseExpr("long2ip(int(-1062731775))").eval(null));
		assertEquals("127.0.0.1", FunctionUtil.parseExpr("long2ip(int(2130706433))").eval(null));
		assertNull( FunctionUtil.parseExpr("long2ip(int(3232235521))").eval(null));

		// long
		assertEquals("192.168.0.1", FunctionUtil.parseExpr("long2ip(long(3232235521))").eval(null));
		assertEquals("127.0.0.1", FunctionUtil.parseExpr("long2ip(long(2130706433))").eval(null));

		// string
		assertNull(FunctionUtil.parseExpr("long2ip(\"3232235521\")").eval(null));
		assertNull(FunctionUtil.parseExpr("long2ip(\"-1062731775\")").eval(null));
		assertNull(FunctionUtil.parseExpr("long2ip(\"2130706433\")").eval(null));
	}
}
