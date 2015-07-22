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
import org.junit.Test;
/**
 * 
 * @author kyun
 *
 */
public class ToLongTest {

	@Test
	public void testLong(){
		ToLong Long = new ToLong(null, expr("10"));
		assertEquals( 10L,  Long.eval(null));
		}
		    
	@Test
	public void testError90840(){
		try {
			new ToLong(null, expr("10", 5));
			fail();
		} catch (QueryParseException e) {
			if (e.isDebugMode()) {
				System.out.println(e.getMessage());
			}
			assertEquals("90840", e.getType());
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
