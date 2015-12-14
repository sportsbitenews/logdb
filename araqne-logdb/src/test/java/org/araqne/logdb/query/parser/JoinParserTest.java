package org.araqne.logdb.query.parser;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.QueryResultFactory;
import org.araqne.logdb.query.command.Join;
import org.araqne.logdb.query.command.Join.JoinType;
import org.araqne.logdb.query.command.Table;
import org.araqne.logdb.query.command.Table.TableParams;
import org.araqne.logdb.query.engine.QueryResultFactoryImpl;
import org.araqne.storage.api.FilePath;
import org.araqne.storage.api.StorageManager;
import org.araqne.storage.api.URIResolver;
import org.araqne.storage.localfile.LocalFilePath;
import org.junit.Test;

public class JoinParserTest {
	static {
		System.setProperty("araqne.data.dir", ".");
	}

	public static class LocalStorageManager implements StorageManager {

		@Override
		public FilePath resolveFilePath(String path) {
			return new LocalFilePath(path);
		}

		@Override
		public void start() {
		}

		@Override
		public void stop() {
		}

		@Override
		public void addURIResolver(URIResolver r) {
			throw new UnsupportedOperationException();
		}

	}

	@Test
	public void testFieldListParse() {
		QueryParserService p = prepareMockQueryParser();
		StorageManager storageManager = new LocalStorageManager();
		QueryResultFactory resultFactory = new QueryResultFactoryImpl(storageManager);
		resultFactory.start();

		JoinParser parser = new JoinParser(p, resultFactory);
		parser.setQueryParserService(p);
		
		{
			String joinCommand = "join ip, port [ table users ]";
			Join join = (Join) parser.parse(null, joinCommand);
			assertEquals(JoinType.Inner, join.getType());
			assertEquals(2, join.getSortFields().length);
			assertEquals("ip", join.getSortFields()[0].getName());
			assertEquals("port", join.getSortFields()[1].getName());
			assertTrue(join.getSortFields()[0].isAsc());
			assertTrue(join.getSortFields()[1].isAsc());

			assertTrue(join.getSubQuery().getCommands().get(0) instanceof Table);
		}

		{
			String joinCommand = "join +ip, port [ table users ]";
			Join join = (Join) parser.parse(null, joinCommand);
			assertEquals(JoinType.Inner, join.getType());
			assertEquals(2, join.getSortFields().length);
			assertEquals("ip", join.getSortFields()[0].getName());
			assertEquals("port", join.getSortFields()[1].getName());
			assertTrue(join.getSortFields()[0].isAsc());
			assertTrue(join.getSortFields()[1].isAsc());

			assertTrue(join.getSubQuery().getCommands().get(0) instanceof Table);
		}

		{
			String joinCommand = "join +ip, +port [ table users ]";
			Join join = (Join) parser.parse(null, joinCommand);
			assertEquals(JoinType.Inner, join.getType());
			assertEquals(2, join.getSortFields().length);
			assertEquals("ip", join.getSortFields()[0].getName());
			assertEquals("port", join.getSortFields()[1].getName());
			assertTrue(join.getSortFields()[0].isAsc());
			assertTrue(join.getSortFields()[1].isAsc());

			assertTrue(join.getSubQuery().getCommands().get(0) instanceof Table);
		}

		{
			String joinCommand = "join ip, +port [ table users ]";
			Join join = (Join) parser.parse(null, joinCommand);
			assertEquals(JoinType.Inner, join.getType());
			assertEquals(2, join.getSortFields().length);
			assertEquals("ip", join.getSortFields()[0].getName());
			assertEquals("port", join.getSortFields()[1].getName());
			assertTrue(join.getSortFields()[0].isAsc());
			assertTrue(join.getSortFields()[1].isAsc());

			assertTrue(join.getSubQuery().getCommands().get(0) instanceof Table);
		}

		{
			String joinCommand = "join ip, +port, seq [ table users ]";
			Join join = (Join) parser.parse(null, joinCommand);
			assertEquals(JoinType.Inner, join.getType());
			assertEquals(3, join.getSortFields().length);
			assertEquals("ip", join.getSortFields()[0].getName());
			assertEquals("port", join.getSortFields()[1].getName());
			assertEquals("seq", join.getSortFields()[2].getName());
			assertTrue(join.getSortFields()[0].isAsc());
			assertTrue(join.getSortFields()[1].isAsc());
			assertTrue(join.getSortFields()[2].isAsc());

			assertTrue(join.getSubQuery().getCommands().get(0) instanceof Table);
		}

		{
			String joinCommand = "join ip, +port, -seq [ table users ]";
			Join join = (Join) parser.parse(null, joinCommand);
			assertEquals(JoinType.Inner, join.getType());
			assertEquals(3, join.getSortFields().length);
			assertEquals("ip", join.getSortFields()[0].getName());
			assertEquals("port", join.getSortFields()[1].getName());
			assertEquals("seq", join.getSortFields()[2].getName());
			assertTrue(join.getSortFields()[0].isAsc());
			assertTrue(join.getSortFields()[1].isAsc());
			assertFalse(join.getSortFields()[2].isAsc());

			assertTrue(join.getSubQuery().getCommands().get(0) instanceof Table);
		}

		{
			String joinCommand = "join -ip, -port, seq [ table users ]";
			Join join = (Join) parser.parse(null, joinCommand);
			assertEquals(JoinType.Inner, join.getType());
			assertEquals(3, join.getSortFields().length);
			assertEquals("ip", join.getSortFields()[0].getName());
			assertEquals("port", join.getSortFields()[1].getName());
			assertEquals("seq", join.getSortFields()[2].getName());
			assertFalse(join.getSortFields()[0].isAsc());
			assertFalse(join.getSortFields()[1].isAsc());
			assertTrue(join.getSortFields()[2].isAsc());

			assertTrue(join.getSubQuery().getCommands().get(0) instanceof Table);
		}
	}

	@Test
	public void testParse() {
		String joinCommand = "join ip [ table users ]";
		QueryParserService p = prepareMockQueryParser();
		StorageManager storageManager = new LocalStorageManager();
		QueryResultFactory resultFactory = new QueryResultFactoryImpl(storageManager);
		resultFactory.start();

		JoinParser parser = new JoinParser(p, resultFactory);
		parser.setQueryParserService(p);
		
		Join join = (Join) parser.parse(null, joinCommand);
		assertEquals(JoinType.Inner, join.getType());
		assertEquals(1, join.getSortFields().length);
		assertEquals("ip", join.getSortFields()[0].getName());
		assertTrue(join.getSortFields()[0].isAsc());

		assertTrue(join.getSubQuery().getCommands().get(0) instanceof Table);
	}

	@Test
	public void testParenthese() {
		QueryParserService p = prepareMockQueryParser();
		StorageManager storageManager = new LocalStorageManager();
		QueryResultFactory resultFactory = new QueryResultFactoryImpl(storageManager);
		resultFactory.start();

		JoinParser parser = new JoinParser(p, resultFactory);
		parser.setQueryParserService(p);
		
		try {
			String joinCommand = "join _id string([table iis])";
			@SuppressWarnings("unused")
			Join join = (Join) parser.parse(null, joinCommand);
			fail();
		} catch (QueryParseException e) {
		}
	}
	
	@Test
	public void testLeftJoinType() {
		QueryParserService p = prepareMockQueryParser();

		StorageManager storageManager = new LocalStorageManager();
		QueryResultFactory resultFactory = new QueryResultFactoryImpl(storageManager);
		resultFactory.start();
		JoinParser parser = new JoinParser(p, resultFactory);
		parser.setQueryParserService(p);
		Join join = (Join) parser.parse(null, "join type=left ip [ table users ]");

		assertEquals(JoinType.Left, join.getType());
		assertEquals(1, join.getSortFields().length);
		assertEquals("ip", join.getSortFields()[0].getName());
		assertTrue(join.getSortFields()[0].isAsc());

		assertTrue(join.getSubQuery().getCommands().get(0) instanceof Table);
	}

	private QueryParserService prepareMockQueryParser() {
		QueryParserService p = mock(QueryParserService.class);
		TableParams params = new TableParams();
		params.setTableSpecs(Arrays.<TableSpec> asList(new WildcardTableSpec("users")));
		QueryCommand table = new Table(params);

		ArrayList<QueryCommand> commands = new ArrayList<QueryCommand>();
		commands.add(table);
		when(p.parseCommands(null, "table users")).thenReturn(commands);
		return p;
	}
}
