package org.araqne.logdb.query.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;

import org.araqne.logdb.LogQueryParseException;
import org.araqne.logdb.query.command.OutputJson;
import org.junit.Test;

/**
 * @since 1.6.7
 * @author darkluster
 * 
 */
public class OutputJsonParserTest {
	@Test
	public void testNormalCase() {
		new File("logexport.json").delete();
		OutputJson json = null;
		try {
			OutputJsonParser p = new OutputJsonParser();
			json = (OutputJson) p.parse(null, "outputjson logexport.json sip, dip ");

			File f = json.getTxtFile();
			assertEquals("logexport.json", f.getName());
			assertEquals("sip", json.getFields().get(0));
			assertEquals("dip", json.getFields().get(1));
		} finally {
			if (json != null)
				json.eof(false);
			new File("logexport.json").delete();
		}
	}

	@Test
	public void testMissingField() {
		new File("logexport.json").delete();
		try {
			OutputJsonParser p = new OutputJsonParser();
			p.parse(null, "outputjson");
			fail();
		} catch (LogQueryParseException e) {
			assertEquals("missing-field", e.getType());
			assertEquals(1, (int) e.getOffset());
		} finally {
			new File("logexport.json").delete();
		}
	}

	@Test
	public void testInvalidEndCharacter() {
		new File("logexport.json").delete();
		try {
			OutputJsonParser p = new OutputJsonParser();
			p.parse(null, "outputjson logexport.json sip,");
			fail();
		} catch (LogQueryParseException e) {
			assertEquals("missing-field", e.getType());
			assertEquals(30, (int) e.getOffset());
		} finally {
			new File("logexport.json").delete();
		}
	}

}
