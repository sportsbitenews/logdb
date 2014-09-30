package org.araqne.logdb;

import org.junit.Test;

import static org.junit.Assert.*;

public class StringsTest {
	@Test
	public void backslashTest() {
		String s = "a\\\\b";
		assertEquals("a\\b", Strings.unescape(s));
	}

	@Test
	public void quoteTest() {
		String s = "a\\\"b";
		assertEquals("a\"b", Strings.unescape(s));
	}

	@Test
	public void newlineTest() {
		String s = "a\\nb";
		assertEquals("a\nb", Strings.unescape(s));
	}

	@Test
	public void tabTest() {
		String s = "a\\tb";
		assertEquals("a\tb", Strings.unescape(s));
	}

	@Test//(expected = QueryParseException.class)
	public void invalidEscapeTest() {
		String value = "a\\bc";
		
		try {
			Strings.unescape(value);
			fail();
		} catch (QueryParseInsideException e) {
			if(e.isDebugMode()){
				System.out.println(value);
				System.out.println(e.getMessage());
			}
			assertEquals("90400", e.getType());
			assertEquals(value, e.getParams().get("value"));
			assertEquals(1,  e.getOffsetS());
			assertEquals(2, e.getOffsetE());
		}
	}
}
