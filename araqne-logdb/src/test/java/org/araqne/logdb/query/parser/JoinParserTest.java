package org.araqne.logdb.query.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;

import org.araqne.logdb.LogQueryCommand;
import org.araqne.logdb.LogQueryParserService;
import org.araqne.logdb.query.command.Join;
import org.araqne.logdb.query.command.Table;
import org.junit.Test;

public class JoinParserTest {
	@Test
	public void testParse() {
		System.setProperty("araqne.data.dir", ".");
		String joinCommand = "join ip [ table users ]";
		LogQueryParserService p = mock(LogQueryParserService.class);
		LogQueryCommand table = new Table(Arrays.asList("users"));

		ArrayList<LogQueryCommand> commands = new ArrayList<LogQueryCommand>();
		commands.add(table);
		when(p.parseCommands(null, "table users")).thenReturn(commands);

		Join join = (Join) new JoinParser(p).parse(null, joinCommand);
		assertEquals(1, join.getSortFields().length);
		assertEquals("ip", join.getSortFields()[0].getName());
		assertTrue(join.getSortFields()[0].isAsc());

		assertTrue(join.getSubQuery().get(0) instanceof Table);
	}
}
