package org.araqne.logdb.query.parser;

import org.araqne.logdb.query.command.Limit;
import org.junit.Test;

import static org.junit.Assert.*;

public class LimitParserTest {
	@Test
	public void testLimitOnly() {
		LimitParser parser = new LimitParser();
		Limit limit = (Limit) parser.parse(null, "limit 10");
		assertEquals(0, limit.getOffset());
		assertEquals(10, limit.getLimit());
	}

	@Test
	public void testOffsetAndLimit() {
		LimitParser parser = new LimitParser();
		Limit limit = (Limit) parser.parse(null, "limit 5 10");
		assertEquals(5, limit.getOffset());
		assertEquals(10, limit.getLimit());
	}
}
