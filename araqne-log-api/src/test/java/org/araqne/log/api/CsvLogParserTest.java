package org.araqne.log.api;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;

public class CsvLogParserTest {

	@Test
	public void testCommaCsvParse() {
		String s = "a,b,c,d,e";
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", s);
		CsvLogParser parser = new CsvLogParser(false, false, null, "line", false);

		Map<String, Object> parsed = parser.parse(m);

		assertEquals("a", parsed.get("column0"));
		assertEquals("b", parsed.get("column1"));
		assertEquals("c", parsed.get("column2"));
		assertEquals("d", parsed.get("column3"));
		assertEquals("e", parsed.get("column4"));
	}

	@Test
	public void testCommaQuoteCsvParse() {
		String s = "\"a\",\"b\",\"c\",\"d\",\"e\"";
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", s);
		CsvLogParser parser = new CsvLogParser(false, false, null, "line", false);

		Map<String, Object> parsed = parser.parse(m);

		assertEquals("a", parsed.get("column0"));
		assertEquals("b", parsed.get("column1"));
		assertEquals("c", parsed.get("column2"));
		assertEquals("d", parsed.get("column3"));
		assertEquals("e", parsed.get("column4"));
	}

	@Test
	public void testCommaQuoteCsvParse2() {
		String s = "\"a\",b,\"c\",d,\"e\"";
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", s);
		CsvLogParser parser = new CsvLogParser(false, false, null, "line", false);

		Map<String, Object> parsed = parser.parse(m);

		assertEquals("a", parsed.get("column0"));
		assertEquals("b", parsed.get("column1"));
		assertEquals("c", parsed.get("column2"));
		assertEquals("d", parsed.get("column3"));
		assertEquals("e", parsed.get("column4"));
	}

	@Test
	public void testTabCsvParse() {
		String s = "a	b	c	d	e";
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", s);
		CsvLogParser parser = new CsvLogParser(true, false, null, "line", false);

		Map<String, Object> parsed = parser.parse(m);

		assertEquals("a", parsed.get("column0"));
		assertEquals("b", parsed.get("column1"));
		assertEquals("c", parsed.get("column2"));
		assertEquals("d", parsed.get("column3"));
		assertEquals("e", parsed.get("column4"));
	}

	@Test
	public void testTabQuoteCsvParse() {
		String s = "\"a\"	\"b\"	\"c\"	\"d\"	\"e\"";
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", s);
		CsvLogParser parser = new CsvLogParser(true, false, null, "line", false);

		Map<String, Object> parsed = parser.parse(m);

		assertEquals("a", parsed.get("column0"));
		assertEquals("b", parsed.get("column1"));
		assertEquals("c", parsed.get("column2"));
		assertEquals("d", parsed.get("column3"));
		assertEquals("e", parsed.get("column4"));
	}

	@Test
	public void testTabQuoteCsvParse2() {
		String s = "\"a\"	\"b\"	c	\"d\"	e";
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", s);
		CsvLogParser parser = new CsvLogParser(true, false, null, "line", false);

		Map<String, Object> parsed = parser.parse(m);

		assertEquals("a", parsed.get("column0"));
		assertEquals("b", parsed.get("column1"));
		assertEquals("c", parsed.get("column2"));
		assertEquals("d", parsed.get("column3"));
		assertEquals("e", parsed.get("column4"));
	}

	@Test
	public void testCsvFieldParse() {
		String s = "a,b,c,d,e";
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", s);
		CsvLogParser parser = new CsvLogParser(false, false, new String[] { "A", "B", "C", "D", "E" }, "line", false);

		Map<String, Object> parsed = parser.parse(m);

		assertEquals("a", parsed.get("A"));
		assertEquals("b", parsed.get("B"));
		assertEquals("c", parsed.get("C"));
		assertEquals("d", parsed.get("D"));
		assertEquals("e", parsed.get("E"));
	}

	@Test
	public void testCsvFieldParse2() {
		String s = "a,b,c,d,e";
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", s);
		CsvLogParser parser = new CsvLogParser(false, false, new String[] { "A", "B" }, "line", false);

		Map<String, Object> parsed = parser.parse(m);

		assertEquals("a", parsed.get("A"));
		assertEquals("b", parsed.get("B"));
		assertEquals("c", parsed.get("column2"));
		assertEquals("d", parsed.get("column3"));
		assertEquals("e", parsed.get("column4"));
	}

	@Test
	public void testEscape() {
		String s = "\"a\",\"b\\\"c\"";
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", s);
		CsvLogParser parser = new CsvLogParser(false, false, null, "line", false);

		Map<String, Object> parsed = parser.parse(m);

		assertEquals("a", parsed.get("column0"));
		assertEquals("b\"c", parsed.get("column1"));
	}

	@Test
	public void testEscape2() {
		String s = "\\\"a, b\\\", c\\\"d , \\\"e\\\"f\\\"g\\\"";
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", s);
		CsvLogParser parser = new CsvLogParser(false, false, null, "line", false);

		Map<String, Object> parsed = parser.parse(m);
		assertEquals("\\\"a", parsed.get("column0"));
		assertEquals(" b\\\"", parsed.get("column1"));
		assertEquals(" c\\\"d ", parsed.get("column2"));
		assertEquals(" \\\"e\\\"f\\\"g\\\"", parsed.get("column3"));
	}

	@Test
	public void testEmptyColumn() {
		String s = ",,,";
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", s);
		CsvLogParser parser = new CsvLogParser(false, false, null, "line", false);

		Map<String, Object> parsed = parser.parse(m);

		assertEquals("", parsed.get("column0"));
		assertEquals("", parsed.get("column1"));
		assertEquals("", parsed.get("column2"));
	}

	@Test
	public void testBlank() {
		String s = " a,b , c ";
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", s);
		CsvLogParser parser = new CsvLogParser(false, false, null, "line", false);

		Map<String, Object> parsed = parser.parse(m);

		assertEquals(" a", parsed.get("column0"));
		assertEquals("b ", parsed.get("column1"));
		assertEquals(" c ", parsed.get("column2"));
	}

	@Ignore
	public void testSpeed() {
		String s = "a,b,c,d,e";
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", s);

		CsvParser parser = new CsvParser(false, false, null);

		int i = 0;
		long start = System.currentTimeMillis();
		while (i < 10000000) {
			parser.parse(s);
			i++;
		}
		System.out.println((System.currentTimeMillis() - start) + "ms elapsed");

		// darkluster pc -> 2789ms elapsed
	}

	// fix for issue#624
	@Test
	public void testBlankFieldName() {
		Map<String, String> configs = new HashMap<String, String>();
		configs.put("column_headers", "a, b, c");

		CsvLogParser parser = (CsvLogParser) new CsvParserFactory().createParser(configs);
		String[] headers = parser.getColumnHeaders();
		assertEquals(3, headers.length);
		assertEquals("a", headers[0]);
		assertEquals("b", headers[1]);
		assertEquals("c", headers[2]);
	}
}
