package org.araqne.logdb.query.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;

import org.araqne.logdb.QueryCommand;
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
	public void testParse() {
		System.setProperty("araqne.data.dir", ".");
		String joinCommand = "join ip [ table users ]";
		QueryParserService p = prepareMockQueryParser();
		StorageManager storageManager = new LocalStorageManager();
		QueryResultFactory resultFactory = new QueryResultFactoryImpl(storageManager);
		resultFactory.start();

		Join join = (Join) new JoinParser(p, resultFactory).parse(null, joinCommand);
		assertEquals(JoinType.Inner, join.getType());
		assertEquals(1, join.getSortFields().length);
		assertEquals("ip", join.getSortFields()[0].getName());
		assertTrue(join.getSortFields()[0].isAsc());

		assertTrue(join.getSubQuery().getCommands().get(0) instanceof Table);
	}

	@Test
	public void testLeftJoinType() {
		QueryParserService p = prepareMockQueryParser();
		StorageManager storageManager = new LocalStorageManager();
		QueryResultFactory resultFactory = new QueryResultFactoryImpl(storageManager);
		resultFactory.start();
		Join join = (Join) new JoinParser(p, resultFactory).parse(null, "join type=left ip [ table users ]");

		assertEquals(JoinType.Left, join.getType());
		assertEquals(1, join.getSortFields().length);
		assertEquals("ip", join.getSortFields()[0].getName());
		assertTrue(join.getSortFields()[0].isAsc());

		assertTrue(join.getSubQuery().getCommands().get(0) instanceof Table);
	}

	private QueryParserService prepareMockQueryParser() {
		QueryParserService p = mock(QueryParserService.class);

		TableParams params = new TableParams();
		params.setTableNames(Arrays.asList("users"));
		QueryCommand table = new Table(params);

		ArrayList<QueryCommand> commands = new ArrayList<QueryCommand>();
		commands.add(table);
		when(p.parseCommands(null, "table users")).thenReturn(commands);
		return p;
	}
}
