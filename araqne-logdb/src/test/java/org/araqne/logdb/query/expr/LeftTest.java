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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;
import org.junit.Test;

/**
 * 
 * @author kyun
 *
 */
public class LeftTest {

	@Test
	public void testLeft() {
		Left left = new Left(null, expr("0123456789", 4));
		assertEquals("0123", left.eval((Row) null));

		left = new Left(null, expr("0123456789", 11));
		assertEquals("0123456789", left.eval((Row) null));

		left = new Left(null, expr("0123456789", 0));
		assertEquals("", left.eval((Row) null));

		left = new Left(null, expr(1234, 2));
		assertEquals("12", left.eval((Row) null));
	}

	@Test
	public void testManual() {
		assertEquals("0123", FunctionUtil.parseExpr("left(\"0123456789\", 4)").eval(null));
		assertEquals("0123456789", FunctionUtil.parseExpr("left(\"0123456789\", 11)").eval(null));
		assertEquals("", FunctionUtil.parseExpr("left(\"0123456789\", 0)").eval(null));
		assertEquals("12", FunctionUtil.parseExpr("left(1234, 2)").eval(null));
		assertEquals("1.2", FunctionUtil.parseExpr("left(1.23, 3)").eval(null));
		assertNull(FunctionUtil.parseExpr("left(int(\"invalid\"), 3)").eval(null));
	}

	@Test
	public void testError90720() {
		try {
			new Left(null, expr("01234", -1));
			fail();
		} catch (QueryParseException e) {
			if (e.isDebugMode()) {
				System.out.println(e.getMessage());
			}
			assertEquals("90720", e.getType());
			assertEquals("-1", e.getParams().get("length"));
		}
	}

	private List<Expression> expr(Object... object) {
		List<Expression> expr = new ArrayList<Expression>();

		for (Object o : object) {
			if (o instanceof Expression)
				expr.add((Expression) o);
			else if (o instanceof String)
				expr.add(new StringConstant((String) o));
			else if (o instanceof Number)
				expr.add(new NumberConstant((Number) o));
			else if (o instanceof Boolean)
				expr.add(new BooleanConstant((Boolean) o));
		}

		return expr;
	}

}
