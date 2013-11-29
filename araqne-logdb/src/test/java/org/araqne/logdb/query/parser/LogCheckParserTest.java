package org.araqne.logdb.query.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.text.SimpleDateFormat;
import java.util.Arrays;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Session;
import org.araqne.logdb.query.command.LogCheck;
import org.araqne.logstorage.LogTableRegistry;
import org.junit.Test;

public class LogCheckParserTest {

	@Test
	public void checkAll() {
		String query = "logcheck";

		LogCheck c = parse(query);
		assertEquals(3, c.getTableNames().size());
		assertTrue(c.getTableNames().contains("secure_log"));
		assertTrue(c.getTableNames().contains("secure_event"));
		assertTrue(c.getTableNames().contains("text_log"));
		assertNull(c.getFrom());
		assertNull(c.getTo());
		assertEquals("logcheck", c.toString());
	}

	@Test
	public void checkAll2() {
		LogCheck c = parse("logcheck *");
		assertEquals(3, c.getTableNames().size());
		assertTrue(c.getTableNames().contains("secure_log"));
		assertTrue(c.getTableNames().contains("secure_event"));
		assertTrue(c.getTableNames().contains("text_log"));
		assertNull(c.getFrom());
		assertNull(c.getTo());
		assertEquals("logcheck *", c.toString());
	}

	@Test
	public void checkSomeTables() {
		String query = "logcheck text_log, secure_log";

		LogCheck c = parse(query);
		assertEquals(2, c.getTableNames().size());
		assertTrue(c.getTableNames().contains("secure_log"));
		assertTrue(c.getTableNames().contains("text_log"));
		assertNull(c.getFrom());
		assertNull(c.getTo());

		assertEquals(query, c.toString());
	}

	@Test
	public void checkWildMatch() {
		String query = "logcheck secure_*";
		LogCheck c = parse(query);
		assertEquals(2, c.getTableNames().size());
		assertTrue(c.getTableNames().contains("secure_log"));
		assertTrue(c.getTableNames().contains("secure_event"));
		assertNull(c.getFrom());
		assertNull(c.getTo());

		assertEquals(query, c.toString());
	}

	@Test
	public void checkFromTo() {
		String query = "logcheck from=20130806 to=20130808 secure_*";
		LogCheck c = parse(query);

		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
		assertEquals(2, c.getTableNames().size());
		assertTrue(c.getTableNames().contains("secure_log"));
		assertTrue(c.getTableNames().contains("secure_event"));
		assertEquals("20130806", df.format(c.getFrom()));
		assertEquals("20130808", df.format(c.getTo()));
		
		assertEquals(query, c.toString());
	}

	private LogCheck parse(String query) {
		LogTableRegistry tableRegistry = mock(LogTableRegistry.class);
		when(tableRegistry.getTableNames()).thenReturn(Arrays.asList("secure_log", "secure_event", "text_log"));

		LogCheckParser parser = new LogCheckParser(tableRegistry, null, null);
		LogCheck c = (LogCheck) parser.parse(getContext(), query);
		return c;
	}

	private QueryContext getContext() {
		Session session = mock(Session.class);
		when(session.isAdmin()).thenReturn(true);
		QueryContext ctx = new QueryContext(session);
		return ctx;
	}

}
