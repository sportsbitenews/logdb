package org.araqne.logdb.query.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.io.File;

import org.araqne.cron.TickService;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.impl.FunctionRegistryImpl;
import org.araqne.logdb.query.command.OutputJson;
import org.araqne.logdb.query.engine.QueryParserServiceImpl;
import org.junit.Before;
import org.junit.Test;

/**
 * @since 1.6.7
 * @author darkluster
 * 
 */
public class OutputJsonParserTest {

	private QueryParserService queryParserService;

	@Before
	public void setup() {
		QueryParserServiceImpl p = new QueryParserServiceImpl();
		p.setFunctionRegistry(new FunctionRegistryImpl());
		queryParserService = p;
	}

	@Test
	public void testNormalCase() {
		new File("logexport.json").delete();
		OutputJson json = null;
		try {
			OutputJsonParser p = new OutputJsonParser(mock(TickService.class));
			p.setQueryParserService(queryParserService);

			json = (OutputJson) p.parse(null, "outputjson logexport.json sip, dip ");

			File f = json.getTxtFile();
			assertEquals("logexport.json", f.getName());
			assertEquals("sip", json.getFields().get(0));
			assertEquals("dip", json.getFields().get(1));

			assertEquals("outputjson logexport.json sip, dip", json.toString());
		} finally {
			if (json != null)
				json.onClose(QueryStopReason.End);
			new File("logexport.json").delete();
		}
	}

	@Test
	public void testMissingField() {
		new File("logexport.json").delete();
		try {
			OutputJsonParser p = new OutputJsonParser(mock(TickService.class));
			p.setQueryParserService(queryParserService);

			p.parse(null, "outputjson");
			fail();
		} catch (QueryParseException e) {
			assertEquals("missing-field", e.getType());
		} finally {
			new File("logexport.json").delete();
		}
	}

	@Test
	public void testInvalidEndCharacter() {
		new File("logexport.json").delete();
		try {
			OutputJsonParser p = new OutputJsonParser(mock(TickService.class));
			p.setQueryParserService(queryParserService);

			p.parse(null, "outputjson logexport.json sip,");
			fail();
		} catch (QueryParseException e) {
			assertEquals("missing-field", e.getType());
			assertEquals(30, (int) e.getOffset());
		} finally {
			new File("logexport.json").delete();
		}
	}

}
