package org.araqne.logdb.query.expr;

import org.junit.Test;
import static org.junit.Assert.*;

public class Ip2LongTest {
	@Test
	public void testIp2Long() {
		// normal case
		assertEquals(16909060L, (long) Ip2Long.convert("1.2.3.4"));

		// corner case
		assertEquals(0L, (long) Ip2Long.convert("0.0.0.0"));
		assertEquals(4294967295L, (long) Ip2Long.convert("255.255.255.255"));

		// invalid chars
		assertNull(Ip2Long.convert("-0.0.0.0"));
		assertNull(Ip2Long.convert("0.0.0,0"));

		// invalid range
		assertNull(Ip2Long.convert("256.0.0.1"));
		assertNull(Ip2Long.convert("2222.0.0.0"));
		assertNull(Ip2Long.convert("22222.11111.0.0"));

		// invalid number count
		assertNull(Ip2Long.convert("1.2.3.4.5"));
		assertNull(Ip2Long.convert("1.2.3.4.5.6"));
	}
}
