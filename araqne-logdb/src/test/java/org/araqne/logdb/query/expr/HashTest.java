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
import org.araqne.logdb.Row;
import org.junit.Test;
/**
 * 
 * @author kyun
 *
 */
public class HashTest {

	@Test
	public void testHash(){
		Hash hash = new Hash(null, expr("md5", binary("hello world")));
		assertEquals("5eb63bbbe01eeed093cb22bb8f5acdc3", byteArrayToHex((byte[])hash.eval((Row) null)));
		
		hash = new Hash(null, expr("sha1", binary("hello world")));
		assertEquals("2aae6c35c94fcfb415dbe95f408b9ce91ee846ed", byteArrayToHex((byte[])hash.eval((Row) null)));
	
		hash = new Hash(null, expr("sha256", binary("hello world")));
		assertEquals("b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9", byteArrayToHex((byte[])hash.eval((Row) null)));
		
		hash = new Hash(null, expr("sha384", binary("hello world")));
		assertEquals("fdbd8e75a67f29f701a4e040385e2e23986303ea10239211af907fcbb83578b3e417cb71ce646efd0819dd8c088de1bd", byteArrayToHex((byte[])hash.eval((Row) null)));
		
		hash = new Hash(null, expr("sha512", binary("hello world")));
		assertEquals("309ecc489c12d6eb4cc40f50c902f2b4d0ed77ee511a7c7a9bcd3ca86d4cd86f989dd35bc5ff499670da34255b45b0cfd830e81f605dcf7dc5542e93ae9cd76f", byteArrayToHex((byte[])hash.eval((Row) null)));
	}
	
	@Test
	public void testError90690(){

		try {
			new Hash(null, expr());
			fail();
		} catch (QueryParseException e) {
			if (e.isDebugMode()) {
				System.out.println(e.getMessage());
			}
			assertEquals("90690", e.getType());
		}
	}

	@Test
	public void testError90691(){

		try {
			new Hash(null, expr("md5"));
			fail();
		} catch (QueryParseException e) {
			if (e.isDebugMode()) {
				System.out.println(e.getMessage());
			}
			assertEquals("90691", e.getType());
		}
	}

	@Test
	public void testError90692(){

		try {
			new Hash(null, expr("md10", "DATA"));
			fail();
		} catch (QueryParseException e) {
			if (e.isDebugMode()) {
				System.out.println(e.getMessage());
			}
			assertEquals("90692", e.getType());
			assertEquals("md10", e.getParams().get("algorithms"));
		}
	}

	private ToBinary binary(String s){
		return new ToBinary(null , expr(s));
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
	
	private String byteArrayToHex(byte[] ba) {
		if (ba == null || ba.length == 0) {
			return null;
		}

		StringBuffer sb = new StringBuffer();
		String hexNumber;
		for (int x = 0; x < ba.length; x++) {
			hexNumber = "0" + Integer.toHexString(0xff & ba[x]);

			sb.append(hexNumber.substring(hexNumber.length() - 2));
		}
		return sb.toString();
	} 

}
