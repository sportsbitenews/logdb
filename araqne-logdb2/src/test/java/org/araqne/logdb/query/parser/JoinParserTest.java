//package org.araqne.logdb.query.parser;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertTrue;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.when;
//
//import java.util.ArrayList;
//import java.util.Arrays;
//
//import org.araqne.logdb.LogQueryCommand;
//import org.araqne.logdb.LogQueryParserService;
//import org.araqne.logdb.query.command.Join;
//import org.araqne.logdb.query.command.Table;
//import org.araqne.logdb.query.command.Join.JoinType;
//import org.araqne.logdb.query.command.Table.TableParams;
//import org.junit.Test;
//
//public class JoinParserTest {
//	@Test
//	public void testParse() {
//		System.setProperty("araqne.data.dir", ".");
//		String joinCommand = "join ip [ table users ]";
//		LogQueryParserService p = prepareMockQueryParser();
//
//		Join join = (Join) new JoinParser(p).parse(null, joinCommand);
//		assertEquals(JoinType.Inner, join.getType());
//		assertEquals(1, join.getSortFields().length);
//		assertEquals("ip", join.getSortFields()[0].getName());
//		assertTrue(join.getSortFields()[0].isAsc());
//
//		assertTrue(join.getSubQuery().get(0) instanceof Table);
//	}
//
//	@Test
//	public void testLeftJoinType() {
//		LogQueryParserService p = prepareMockQueryParser();
//		Join join = (Join) new JoinParser(p).parse(null, "join type=left ip [ table users ]");
//
//		assertEquals(JoinType.Left, join.getType());
//		assertEquals(1, join.getSortFields().length);
//		assertEquals("ip", join.getSortFields()[0].getName());
//		assertTrue(join.getSortFields()[0].isAsc());
//
//		assertTrue(join.getSubQuery().get(0) instanceof Table);
//	}
//
//	private LogQueryParserService prepareMockQueryParser() {
//		LogQueryParserService p = mock(LogQueryParserService.class);
//
//		TableParams params = new TableParams();
//		params.setTableNames(Arrays.asList("users"));
//		LogQueryCommand table = new Table(params);
//
//		ArrayList<LogQueryCommand> commands = new ArrayList<LogQueryCommand>();
//		commands.add(table);
//		when(p.parseCommands(null, "table users")).thenReturn(commands);
//		return p;
//	}
//}
