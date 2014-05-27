package org.araqne.logdb.query.parser;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.Session;
import org.araqne.logdb.impl.FunctionRegistryImpl;
import org.araqne.logdb.query.command.Import;
import org.araqne.logdb.query.engine.QueryParserServiceImpl;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogTableRegistry;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ImportParserTest {
	
	private QueryParserService queryParserService;

	@Before
	public void setup() {
		QueryParserServiceImpl p = new QueryParserServiceImpl();
		p.setFunctionRegistry(new FunctionRegistryImpl());
		queryParserService = p;
	}
	
	@Test
	public void parseTableExists() {
		LogTableRegistry tableRegistry = mock(LogTableRegistry.class);
		LogStorage storage = mock(LogStorage.class);
		Session session = mock(Session.class);
		when(session.isAdmin()).thenReturn(true);
		when(tableRegistry.exists("sample")).thenReturn(true);

		QueryContext context = new QueryContext(session);
		ImportParser p = new ImportParser(tableRegistry, storage);
		p.setQueryParserService(queryParserService);
		
		Import imp = (Import) p.parse(context, "import sample");
		assertEquals("sample", imp.getTableName());
		assertEquals(false, imp.isCreate());
	}

	/**
	 * @since 1.8.2
	 */
	@Test
	public void parseCreateOption() {
		LogTableRegistry tableRegistry = mock(LogTableRegistry.class);
		LogStorage storage = mock(LogStorage.class);
		Session session = mock(Session.class);
		when(session.isAdmin()).thenReturn(true);
		when(tableRegistry.exists("sample")).thenReturn(false);

		QueryContext context = new QueryContext(session);
		ImportParser p = new ImportParser(tableRegistry, storage);
		p.setQueryParserService(queryParserService);
		
		Import imp = (Import) p.parse(context, "import create=true sample");
		assertEquals("sample", imp.getTableName());
		assertTrue(imp.isCreate());
	}

	@Test(expected = QueryParseException.class)
	public void parseNoPermission() {
		LogTableRegistry tableRegistry = mock(LogTableRegistry.class);
		LogStorage storage = mock(LogStorage.class);
		Session session = mock(Session.class);
		when(session.isAdmin()).thenReturn(false);
		when(tableRegistry.exists("sample")).thenReturn(true);

		QueryContext context = new QueryContext(session);
		ImportParser p = new ImportParser(tableRegistry, storage);
		p.setQueryParserService(queryParserService);
		
		p.parse(context, "import create=true sample");
	}

}
