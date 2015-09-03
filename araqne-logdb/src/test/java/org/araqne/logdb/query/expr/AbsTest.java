/**
 * Copyright 2014 Eediom Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.araqne.logdb.query.expr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.Row;
import org.araqne.logdb.impl.FunctionRegistryImpl;
import org.araqne.logdb.query.command.Eval;
import org.araqne.logdb.query.engine.QueryParserServiceImpl;
import org.araqne.logdb.query.parser.EvalParser;
import org.junit.Before;
import org.junit.Test;
/**
 * 
 * @author kyun
 *
 */
public class AbsTest {
	private QueryParserService queryParserService;

	@Before
	public void setup() {
		QueryParserServiceImpl p = new QueryParserServiceImpl();
		p.setFunctionRegistry(new FunctionRegistryImpl());
		queryParserService = p;
	}
	
	@Test
	public void testAbs(){
		Abs abs = new Abs(null, expr(1));
		assertEquals(1, abs.eval(null));
	}
	
	@Test
	public void testAsbByEval() {
		EvalParser p = new EvalParser();
		p.setQueryParserService(queryParserService);

		Eval eval = (Eval) p.parse(null, "eval n=abs(-1)");
		Row row = new Row();
		eval.onPush(row);
		assertEquals(1, row.map().size());
		assertEquals(1, row.get("n"));
	}

	@Test
	public void testError90600(){
		
		try {
			 new Abs(null, expr(1, 2, 3));
			fail();
		} catch (QueryParseException e) {
			if (e.isDebugMode()) {
				System.out.println(e.getMessage());
			}
			assertEquals("90600", e.getType());
		}
	}
	
	@Test
	public void testError90600ByEval() {
		EvalParser p = new EvalParser();
		p.setQueryParserService(queryParserService);
		
		try {
			p.parse(null, "eval n=abs( -1,  2)");
			fail();
		} catch (QueryParseException e) {
			if (e.isDebugMode()) {
				System.out.println(e.getMessage());
			}
			assertEquals("90600", e.getType());
			assertEquals("n=abs( -1,  2)", e.getParams().get("value"));
		}
	}

	private List<Expression> expr(Number... number){
		List<Expression> expr = new ArrayList<Expression>();

		for(Number n : number )
			expr.add(new NumberConstant(n));
		
		return expr;
	}
	
}
