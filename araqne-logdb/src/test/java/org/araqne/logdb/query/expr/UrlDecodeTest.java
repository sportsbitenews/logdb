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
import org.junit.Ignore;
import org.junit.Test;
/**
 * 
 * @author kyun
 *
 */
public class UrlDecodeTest {

	@Ignore
	@Test
	public void testUrlDecode(){
		UrlDecode urldecode = new UrlDecode(null, expr("%EB%A1%9C%EA%B7%B8%EB%B6%84%EC%84%9D"));
		assertEquals( "로그분석",  urldecode.eval(null));
	}
		    
	@Test
	public void testError90850(){
		try {
			new UrlDecode(null, expr("test", "euc-kk"));
			fail();
		} catch (QueryParseException e) {
			if (e.isDebugMode()) {
				System.out.println(e.getMessage());
			}
			assertEquals("90850", e.getType());
			assertEquals("euc-kk", e.getParams().get("charset"));
		}
	}
	
	@Test
	public void testManual() {
		assertNull(FunctionUtil.parseExpr("urldecode(int(\"invalid\"))").eval(null));
		assertEquals("로그분석", FunctionUtil.parseExpr("urldecode(\"%B7%CE%B1%D7%BA%D0%BC%AE\", \"EUC-KR\")").eval(null));
		assertEquals("로그분석", FunctionUtil.parseExpr("urldecode(\"%EB%A1%9C%EA%B7%B8%EB%B6%84%EC%84%9D\")").eval(null));
	}
	
	private List<Expression> expr(Object...object ){
		List<Expression> expr = new ArrayList<Expression>();

		for(Object o: object){
			if(o instanceof Expression)
				expr.add((Expression)o);
			else if(o instanceof String)
				expr.add(new StringConstant((String)o));
			else if(o instanceof Number)
				expr.add(new NumberConstant((Number)o));
			else if(o instanceof Boolean)
				expr.add(new BooleanConstant((Boolean)o));
		}

		return expr;

	}
}
