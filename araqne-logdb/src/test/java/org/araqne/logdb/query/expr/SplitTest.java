package org.araqne.logdb.query.expr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.List;

import org.araqne.logdb.Row;
import org.araqne.logdb.Strings;
import org.junit.Test;

public class SplitTest {
	@Test
	public void singleDelimiterTest() {
		List<String> result = getResult(",", "hello,world");
		assertEquals(2, result.size());
		assertEquals("hello", result.get(0));
		assertEquals("world", result.get(1));
	}

	@Test
	public void singleDelimiterBlankTokenTest() {
		List<String> result = getResult(",", "qoo,,foo");
		assertEquals(3, result.size());
		assertEquals("qoo", result.get(0));
		assertEquals("", result.get(1));
		assertEquals("foo", result.get(2));
	}

	@Test
	public void multiDelimitersTest() {
		List<String> result = getResult("'|,", "1'|,2'|,3'|,4");
		assertEquals(4, result.size());
		assertEquals("1", result.get(0));
		assertEquals("2", result.get(1));
		assertEquals("3", result.get(2));
		assertEquals("4", result.get(3));
	}

	@Test
	public void multiDelimitersBlankTokenTest() {
		List<String> result = getResult("'|,", "1'|,2'|,3'|,'|,4");
		assertEquals(5, result.size());
		assertEquals("1", result.get(0));
		assertEquals("2", result.get(1));
		assertEquals("3", result.get(2));
		assertEquals("", result.get(3));
		assertEquals("4", result.get(4));
	}

	@Test
	public void emptyTargetTest() {
		List<String> result = getResult(",", "");
		assertEquals(0, result.size());
	}

	@Test
	public void emptyNullTest() {
		List<String> result = getResult(",", null);
		assertNull(result);
	}

	@Test
	public void spaceTokensTest() {
		List<String> result = getResult(",", " , , , , , ");
		assertEquals(6, result.size());
		assertEquals(" ", result.get(0));
		assertEquals(" ", result.get(1));
		assertEquals(" ", result.get(2));
		assertEquals(" ", result.get(3));
		assertEquals(" ", result.get(4));
		assertEquals(" ", result.get(5));
	}

	@Test
	public void emptyTokensTest() {
		List<String> result = getResult(",", ",,,,,");
		assertEquals(6, result.size());
		assertEquals("", result.get(0));
		assertEquals("", result.get(1));
		assertEquals("", result.get(2));
		assertEquals("", result.get(3));
		assertEquals("", result.get(4));
		assertEquals("", result.get(5));
	}

	@Test
	public void includeSpaceTokensTest() {
		List<String> result = getResult(",", "a ,b , c, d , e e ,");
		assertEquals(6, result.size());
		assertEquals("a ", result.get(0));
		assertEquals("b ", result.get(1));
		assertEquals(" c", result.get(2));
		assertEquals(" d ", result.get(3));
		assertEquals(" e e ", result.get(4));
		assertEquals("", result.get(5));
	}

	@Test
	public void tabSplitTest() {
		List<String> result = getResult(Strings.unescape("\\t"), "a\tb\tc");
		System.out.println(result);
	}

	@Test
	public void includeSpaceDelimiterTest() {
		List<String> result = getResult(", ", "a, b, c, d, e");
		assertEquals(5, result.size());
		assertEquals("a", result.get(0));
		assertEquals("b", result.get(1));
		assertEquals("c", result.get(2));
		assertEquals("d", result.get(3));
		assertEquals("e", result.get(4));
	}

	@SuppressWarnings("unchecked")
	private List<String> getResult(String delimiter, String line) {
		Expression arg1 = (Expression) new Field(null, Arrays.asList((Expression) new StringConstant("line")));
		Expression arg2 = (Expression) new StringConstant(delimiter);
		List<Expression> exprs = Arrays.asList(arg1, arg2);
		Split split = new Split(null, exprs);
		Row row = new Row();
		row.put("line", line);
		Object o = split.eval(row);
		if (o == null)
			return null;

		return (List<String>) o;
	}
}
