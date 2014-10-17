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

import org.junit.Test;

public class ConcatTest {
	@Test
	public void testManual() {
		assertEquals("helloworld", FunctionUtil.parseExpr("concat(\"hello\", \"world\")").eval(null));
		assertEquals("abc", FunctionUtil.parseExpr("concat(\"a\", \"b\", \"c\")").eval(null));
	}
}
