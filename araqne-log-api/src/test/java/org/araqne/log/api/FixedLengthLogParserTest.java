package org.araqne.log.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class FixedLengthLogParserTest {

	@Test
	public void testSameLength() {
		Integer[] fieldLength = new Integer[] { 10, 3, 7 };
		String[] header = new String[] { "Severity", "Count", "Id" };

		String line = "Emergency 013test10 ";
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", line);
		FixedLengthLogParser p = new FixedLengthLogParser("line", false, fieldLength, header);

		Map<String, Object> result = p.parse(m);

		assertEquals("Emergency", result.get("Severity"));
		assertEquals("013", result.get("Count"));
		assertEquals("test10", result.get("Id"));
	}

	@Test
	public void testFieldLength() {
		Integer[] fieldLength = new Integer[] { 10 };
		String[] header = new String[] { "Severity", "Count", "Id" };

		String line = "Emergency 013test10 ";
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", line);
		FixedLengthLogParser p = new FixedLengthLogParser("line", false, fieldLength, header);

		Map<String, Object> result = p.parse(m);

		assertEquals("Emergency", result.get("Severity"));
		assertTrue(result.containsKey("Count"));
		assertTrue(result.containsKey("Id"));
		assertNull(result.get("Count"));
		assertNull(result.get("Id"));
	}

	@Test
	public void testHeaderLength() {
		Integer[] fieldLength = new Integer[] { 10, 3, 7 };
		String[] header = new String[] { "Severity" };

		String line = "Emergency 013test10 ";
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", line);
		FixedLengthLogParser p = new FixedLengthLogParser("line", false, fieldLength, header);

		Map<String, Object> result = p.parse(m);

		assertEquals("Emergency", result.get("Severity"));
		assertFalse(result.containsKey("Count"));
		assertFalse(result.containsKey("Id"));
		assertNull(result.get("Count"));
		assertNull(result.get("Id"));
	}

	@Test
	public void testShortLength() {
		Integer[] fieldLength = new Integer[] { 10, 3, 7 };
		String[] header = new String[] { "Severity", "Count", "Id" };

		String line = "Emerge";
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", line);
		FixedLengthLogParser p = new FixedLengthLogParser("line", false, fieldLength, header);

		Map<String, Object> result = p.parse(m);

		assertEquals("Emerge", result.get("Severity"));
		assertTrue(result.containsKey("Count"));
		assertTrue(result.containsKey("Id"));
		assertNull(result.get("Count"));
		assertNull(result.get("Id"));
	}

	@Test
	public void testLongLength() {
		Integer[] fieldLength = new Integer[] { 10, 3, 7 };
		String[] header = new String[] { "Severity", "Count", "Id" };

		String line = "Emergency 013test10 sad12f12vvvb 123qa`1";
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", line);
		FixedLengthLogParser p = new FixedLengthLogParser("line", false, fieldLength, header);

		Map<String, Object> result = p.parse(m);

		assertEquals("Emergency", result.get("Severity"));
		assertEquals("013", result.get("Count"));
		assertEquals("test10", result.get("Id"));
	}
}
