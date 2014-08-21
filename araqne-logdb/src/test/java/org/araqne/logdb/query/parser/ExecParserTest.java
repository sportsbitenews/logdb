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
package org.araqne.logdb.query.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.impl.FunctionRegistryImpl;
import org.araqne.logdb.query.command.Exec;
import org.araqne.logdb.query.engine.QueryParserServiceImpl;
import org.araqne.logdb.query.expr.EvalField;
import org.araqne.logdb.query.expr.Expression;
import org.araqne.logdb.query.expr.StringConstant;
import org.junit.Before;
import org.junit.Test;

public class ExecParserTest {
	private ExecParser parser;
	private QueryParserService queryParserService;

	@Before
	public void setup() {
		QueryParserServiceImpl p = new QueryParserServiceImpl();
		p.setFunctionRegistry(new FunctionRegistryImpl());
		queryParserService = p;
		parser = new ExecParser();
		parser.setQueryParserService(queryParserService);
	}

	@Test
	public void testCommandOnly() {
		Exec exec = (Exec) parser.parse(null, "exec run.bat");
		assertEquals("run.bat", exec.getCommand());
	}

	@Test
	public void testCommandWithConstOpts() {
		Exec exec = (Exec) parser.parse(null, "exec run.bat \"c:\\\\windows\\\\media\\\\chimes.wav\"");
		assertEquals("run.bat", exec.getCommand());

		Expression arg1 = exec.getArguments().get(0);
		assertTrue(arg1 instanceof StringConstant);
		assertEquals("c:\\windows\\media\\chimes.wav", arg1.eval(null));
	}

	@Test
	public void testCommandWithFields() {
		Exec exec = (Exec) parser.parse(null, "exec run.bat path");
		assertEquals("run.bat", exec.getCommand());

		Expression arg1 = exec.getArguments().get(0);
		assertTrue(arg1 instanceof EvalField);
		assertEquals("path", arg1.toString());
	}

	@Test(expected = QueryParseException.class)
	public void testBrokenFields() {
		Exec exec = (Exec) parser.parse(null, "exec run.bat path, ");
		assertEquals("run.bat", exec.getCommand());

		Expression arg1 = exec.getArguments().get(0);
		assertTrue(arg1 instanceof EvalField);
		assertEquals("path", arg1.toString());
	}

}
