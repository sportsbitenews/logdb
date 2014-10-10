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

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.araqne.logdb.QueryParseException;
import org.junit.Test;
/**
 * 
 * @author kyun
 *
 */
public class ToBinaryTest {

	@Test
	public void testBinary(){
		ToBinary binary = new ToBinary(null, expr("0123456789"));
		assertEquals( "0123456789",  new String((byte [])binary.eval(null)));
	}
	
	@Test
	public void testCharSet() throws UnsupportedEncodingException{
		ToBinary binary = new ToBinary(null, expr("abcd1234", "UTF-8"));
		assertEquals( "abcd1234",  new String((byte []) binary.eval(null), "UTF-8"));
	}
	    
	@Test
	public void testError90810(){
		try {
			new ToBinary(null, expr());
			fail();
		} catch (QueryParseException e) {
			if (e.isDebugMode()) {
				System.out.println(e.getMessage());
			}
			assertEquals("90810", e.getType());
		}
	}
	
	@Test
	public void testError90811(){
		try {
			new ToBinary(null, expr("01234", "euc-ke"));
			fail();
		} catch (QueryParseException e) {
			if (e.isDebugMode()) {
				System.out.println(e.getMessage());
			}
			assertEquals("90811", e.getType());
			assertEquals("euc-ke", e.getParams().get("charset"));
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
