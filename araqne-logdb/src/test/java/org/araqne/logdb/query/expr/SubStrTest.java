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

import org.araqne.logdb.QueryParseInsideException;
import org.junit.Test;
/**
 * 
 * @author kyun
 *
 */
public class SubStrTest {

	@Test
	public void testSubStr(){
		Substr substr = new Substr(null, expr("0123456789", 0, 3));
		assertEquals("012", substr.eval(null));
	}
	    
	@Test
	public void testError90790(){
		try {
			new Substr(null, expr("01234", -1, 2));
			fail();
		} catch (QueryParseInsideException e) {
			if (e.isDebugMode()) {
				System.out.println(e.getMessage());
			}
			assertEquals("90790", e.getType());
			assertEquals("-1", e.getParams().get("begin"));
		}
	}
	
	@Test
	public void testError90791(){
		try {
			new Substr(null, expr("01234", 3,1));
			fail();
		} catch (QueryParseInsideException e) {
			if (e.isDebugMode()) {
				System.out.println(e.getMessage());
			}
			assertEquals("90791", e.getType());
			assertEquals("3", e.getParams().get("begin"));
			assertEquals("1", e.getParams().get("end"));
		}
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
