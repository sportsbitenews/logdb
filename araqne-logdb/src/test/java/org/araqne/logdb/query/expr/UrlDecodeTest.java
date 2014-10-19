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

import org.junit.Test;

public class UrlDecodeTest {
	@Test
	public void testManual() {
		assertNull(FunctionUtil.parseExpr("urldecode(int(\"invalid\"))").eval(null));
		assertEquals("로그분석", FunctionUtil.parseExpr("urldecode(\"%B7%CE%B1%D7%BA%D0%BC%AE\", \"EUC-KR\")").eval(null));
		assertEquals("로그분석", FunctionUtil.parseExpr("urldecode(\"%EB%A1%9C%EA%B7%B8%EB%B6%84%EC%84%9D\")").eval(null));
	}
}
