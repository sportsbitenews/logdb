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
public class EncryptTest {
	private String key = "mRcOlK9V47rjVL/RBYQYRw==";
	private String algorithm = "AES";

	@Test
	public void testEncrypt(){
		assertEquals("helloworld", decrypt(encrypt( "helloworld")));
	}

	@Test
	public void testError90660(){
		//args size 1
		try {
			new Encrypt(null, expr("AES"));
			fail();
		} catch (QueryParseException e) {
			if (e.isDebugMode()) {
				System.out.println(e.getMessage());
			}
			assertEquals("90660", e.getType());
		}

		//args size 2
		try {
			new Encrypt(null, expr("AES", "KEY"));
			fail();
		} catch (QueryParseException e) {
			if (e.isDebugMode()) {
				System.out.println(e.getMessage());
			}
			assertEquals("90660", e.getType());
		}
	}

	@Test
	public void testError90661(){
		try {
			new Encrypt(null, expr("ASE", key, "hello"));
			fail();
		} catch (QueryParseException e) {
			if (e.isDebugMode()) {
				System.out.println(e.getMessage());
			}
			assertEquals("90661", e.getType());
			assertEquals("ASE", e.getParams().get("algorithm"));
		}
	}

	private String encrypt(String plain){
		return  new ToBase64(null, expr(new Encrypt(null, expr(algorithm, frombase64(key), binary(plain))))).eval(null).toString();
	}

	private String decrypt(String enc){
		return new Decode(null, expr(new Decrypt(null, expr(algorithm, frombase64(key), frombase64(enc))))).eval(null).toString();
	}

	private FromBase64 frombase64(String s){
		return new FromBase64(null, expr(s));
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

}
