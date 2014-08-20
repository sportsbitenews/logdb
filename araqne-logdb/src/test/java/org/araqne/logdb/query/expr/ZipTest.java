package org.araqne.logdb.query.expr;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.FunctionRegistry;
import org.araqne.logdb.Row;
import org.araqne.logdb.impl.FunctionRegistryImpl;
import org.araqne.logdb.query.parser.ExpressionParser;
import org.junit.Test;

/**
 * @since 2.4.22
 * @author kyun
 */
public class ZipTest {

	private Row row;

	public ZipTest() {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("l1", Arrays.asList(1, 2, 3, 4));
		map.put("l2", Arrays.asList("a", "b", "c", "d"));
		map.put("l3", Arrays.asList(1, 2, 3, 4, 5));
		map.put("l4", Arrays.asList("a", "b", "c"));
		map.put("null1", null);
		map.put("null2", null);
		map.put("s1", "scalar1");
		map.put("s2", "scalar2");
		row = new Row(map);
	}

	@Test
	public void simpleZipTest() {
		testZip("[[1, a], [2, b], [3, c], [4, d]]", "l1","l2");
	}

	@Test
	public void nullZipTest() {
		testZip(null, "null1","null2");
	}

	@Test
	public void scalarZipTest() {
		testZip("[[scalar1, scalar2]]", "s1","s2");
	}

	@Test
	public void complexZipTest() {
		testZip("[[1, a], [2, b], [3, c], [4, null], [5, null]]", "l3","l4");
		testZip("[[1, null], [2, null], [3, null], [4, null]]", "l1","null1");
		testZip("[[1, null], [2, null], [3, null], [4, null]]", "l1", "null1");
		testZip("[[null, 1], [null, 2], [null, 3], [null, 4]]", "null1", "l1");
		testZip("[[1, scalar1], [2, null], [3, null], [4, null]]", "l1", "s1");
		testZip("[[scalar1, 1], [null, 2], [null, 3], [null, 4]]", "s1", "l1");
		testZip("[[scalar1, 1, null], [null, 2, null], [null, 3, null], [null, 4, null]]", "s1", "l1", "null1" );
		testZip("[[1, a, 1, a, null, null, scalar1, scalar2], [2, b, 2, b, null, null, null, null], "
				+ "[3, c, 3, c, null, null, null, null], [4, d, 4, null, null, null, null, null], "
				+ "[null, null, 5, null, null, null, null, null]]", "l1", "l2", "l3", "l4", "null1", "null2", "s1", "s2" );
		
	}

	private void testZip(Object expected, String... fields){
		List<Expression> exprs = new ArrayList<Expression>();
		for(String field : fields)
			exprs.add(parseExpr(field));

		Zip zip = new Zip(null, exprs);	
		Object actual = zip.eval(row);
		if(actual instanceof List)
			actual = ((List<?>) zip.eval(row)).toString();
		assertEquals(expected, actual);
	}

	private FunctionRegistry funcRegistry = new FunctionRegistryImpl();

	private Expression parseExpr(String expr) {
		return ExpressionParser.parse(null, expr, funcRegistry);
	}

}
