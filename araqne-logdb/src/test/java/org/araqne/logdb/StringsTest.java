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

	@Test(expected = QueryParseException.class)
	public void invalidEscapeTest() {
		Strings.unescape("a\\bb");
	}
}
