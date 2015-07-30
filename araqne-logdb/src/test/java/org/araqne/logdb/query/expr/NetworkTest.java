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
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.Row;
import org.junit.Test;

/**
 * 
 * @author kyun
 * 
 */
public class NetworkTest {

	@Test
	public void testConstantMask() {
		assertEquals("1.2.3.0", test("1.2.3.4", 24));
		assertEquals("1.2.0.0", test("1.2.3.4", 16));
		assertNull(test("1.2.3.4", 100));
	}

	@Test
	public void testVariableMask() {
		Network func = new Network(null, expr(field("ip"), field("mask")));
		Map<String, Object> row = new HashMap<String, Object>();
		row.put("ip", "1.2.3.4");
		row.put("mask", 16);
		assertEquals("1.2.0.0", func.eval(new Row(row)));
	}

	private Expression field(String s) {
		return new Field(null, expr(s));
	}

	private String test(String ip, Object mask) {
		Network func = new Network(null, expr(new Field(null, expr("ip")), mask));
		return (String) func.eval(ip(ip));
	}

	private Row ip(String ip) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("ip", "1.2.3.4");
		return new Row(m);
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
