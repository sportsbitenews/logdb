package org.araqne.logdb.query.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.impl.FunctionRegistryImpl;
import org.araqne.logdb.query.command.Eval;
import org.araqne.logdb.query.engine.QueryParserServiceImpl;
import org.junit.Before;
import org.junit.Test;

public class EvalParserTest {
	private QueryParserService queryParserService;

	@Before
	public void setup() {
		QueryParserServiceImpl p = new QueryParserServiceImpl();
		p.setFunctionRegistry(new FunctionRegistryImpl());
		queryParserService = p;
	}

	@Test
	public void testConstantExpr() {
		EvalParser p = new EvalParser();
		p.setQueryParserService(queryParserService);

		Eval eval = (Eval) p.parse(null, "eval n=1+2+min(10, 4, 34, -9)");
		Object o = eval.getExpression().eval(null);
		assertEquals(-6L, o);
	}

	@Test
	public void testBrokenEval1() {
		EvalParser p = new EvalParser();
		p.setQueryParserService(queryParserService);

		try {
			p.parse(null, "eval ");
			fail();
		} catch (QueryParseException e) {
			assertEquals("assign-token-not-found", e.getType());
		}
	}

	@Test
	public void testBrokenEval2() {
		EvalParser p = new EvalParser();
		p.setQueryParserService(queryParserService);

		try {
			p.parse(null, "eval =");
			fail();
		} catch (QueryParseException e) {
			assertEquals("field-name-not-found", e.getType());
		}
	}

	@Test
	public void testBrokenEval3() {
		EvalParser p = new EvalParser();
		p.setQueryParserService(queryParserService);

		try {
			p.parse(null, "eval n=");
			fail();
		} catch (QueryParseException e) {
			assertEquals("expression-not-found", e.getType());
		}
	}

	@Test
	public void testEvalQueryGeneration() {
		EvalParser p = new EvalParser();
		p.setQueryParserService(queryParserService);

		Eval eval = (Eval) p.parse(null, "eval n=1+2");
		assertEquals("eval n=(1 + 2)", eval.toString());
	}
}
