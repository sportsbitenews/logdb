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

import java.util.Arrays;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.impl.FunctionRegistryImpl;
import org.araqne.logdb.query.parser.ExpressionParser;
import org.junit.Test;

import static org.junit.Assert.*;

public class ModTest {
	@Test
	public void testMod() {
		Expression num = new NumberConstant(10);
		Expression divisor = new NumberConstant(3);
		Object ret = evaluate(num, divisor);
		assertEquals(1L, ret);
	}

	@Test
	public void testDivideByZero() {
		Expression num = new NumberConstant(10);
		Expression divisor = new NumberConstant(0);
		Object ret = evaluate(num, divisor);
		assertNull(ret);
	}

	@Test
	public void testNullArgs() {
		Expression num = ExpressionParser.parse(new QueryContext(null), "null", new FunctionRegistryImpl());
		Expression divisor = new NumberConstant(3);
		Object ret = evaluate(num, divisor);
		assertNull(ret);
	}

	@Test
	public void testNonIntegerArgs() {
		Expression num = new NumberConstant(10.3f);
		Expression divisor = new NumberConstant(3);
		Object ret = evaluate(num, divisor);
		assertNull(ret);
	}

	@Test
	public void testNonNumberArgs() {
		Expression num = new StringConstant("hello");
		Expression divisor = new NumberConstant(3);
		Object ret = evaluate(num, divisor);
		assertNull(ret);
	}

	private Object evaluate(Expression num, Expression divisor) {
		Mod mod = new Mod(new QueryContext(null), Arrays.asList(num, divisor));
		Object ret = mod.eval(new Row());
		return ret;
	}
}
