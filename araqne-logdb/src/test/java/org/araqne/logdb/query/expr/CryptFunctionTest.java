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

public class CryptFunctionTest {
	@Test
	public void testDecodeManual() {
		assertEquals("helloworld", FunctionUtil.parseExpr("decode(frombase64(\"aGVsbG93b3JsZA==\"))").eval(null));
	}

	@Test
	public void testDecryptManual() {
		assertEquals(
				"helloworld",
				FunctionUtil.parseExpr(
						"decode(decrypt(\"AES\", frombase64(\"mRcOlK9V47rjVL/RBYQYRw==\"), frombase64(\"2W3kHbN95/HUSTE/bJt/8g==\")))")
						.eval(null));
	}
	
	@Test
	public void testEncodeManual() {
		assertEquals("aGVsbG93b3JsZA==", FunctionUtil.parseExpr("tobase64(encode(\"helloworld\"))").eval(null));
	}

	@Test
	public void testEncryptManual() {
		assertEquals("2W3kHbN95/HUSTE/bJt/8g==",
				FunctionUtil.parseExpr("tobase64(encrypt(\"AES\", frombase64(\"mRcOlK9V47rjVL/RBYQYRw==\"), binary(\"helloworld\")))")
						.eval(null));
	}
	
	@Test
	public void testHashManual() {
		assertArrayEquals(new byte[] { (byte) 0x5e, (byte) 0xb6, (byte) 0x3b, (byte) 0xbb, (byte) 0xe0, (byte) 0x1e, (byte) 0xee,
				(byte) 0xd0, (byte) 0x93, (byte) 0xcb, (byte) 0x22, (byte) 0xbb, (byte) 0x8f, (byte) 0x5a, (byte) 0xcd, (byte) 0xc3 },
				(byte[]) (FunctionUtil.parseExpr("hash(\"md5\", binary(\"hello world\"))").eval(null)));
		assertArrayEquals(new byte[] { (byte) 0x2a, (byte) 0xae, (byte) 0x6c, (byte) 0x35, (byte) 0xc9, (byte) 0x4f, (byte) 0xcf, (byte) 0xb4,
				(byte) 0x15, (byte) 0xdb, (byte) 0xe9, (byte) 0x5f, (byte) 0x40, (byte) 0x8b, (byte) 0x9c, (byte) 0xe9, (byte) 0x1e,
				(byte) 0xe8, (byte) 0x46, (byte) 0xed },
				(byte[]) (FunctionUtil.parseExpr("hash(\"sha1\", binary(\"hello world\"))").eval(null)));
		assertArrayEquals(new byte[] { (byte) 0xb9, (byte) 0x4d, (byte) 0x27, (byte) 0xb9, (byte) 0x93, (byte) 0x4d, (byte) 0x3e, (byte) 0x08,
				(byte) 0xa5, (byte) 0x2e, (byte) 0x52, (byte) 0xd7, (byte) 0xda, (byte) 0x7d, (byte) 0xab, (byte) 0xfa, (byte) 0xc4,
				(byte) 0x84, (byte) 0xef, (byte) 0xe3, (byte) 0x7a, (byte) 0x53, (byte) 0x80, (byte) 0xee, (byte) 0x90, (byte) 0x88,
				(byte) 0xf7, (byte) 0xac, (byte) 0xe2, (byte) 0xef, (byte) 0xcd, (byte) 0xe9 },
				(byte[]) (FunctionUtil.parseExpr("hash(\"sha256\", binary(\"hello world\"))").eval(null)));
		assertArrayEquals(new byte[] { (byte) 0xfd, (byte) 0xbd, (byte) 0x8e, (byte) 0x75, (byte) 0xa6, (byte) 0x7f, (byte) 0x29, (byte) 0xf7,
				(byte) 0x01, (byte) 0xa4, (byte) 0xe0, (byte) 0x40, (byte) 0x38, (byte) 0x5e, (byte) 0x2e, (byte) 0x23, (byte) 0x98,
				(byte) 0x63, (byte) 0x03, (byte) 0xea, (byte) 0x10, (byte) 0x23, (byte) 0x92, (byte) 0x11, (byte) 0xaf, (byte) 0x90,
				(byte) 0x7f, (byte) 0xcb, (byte) 0xb8, (byte) 0x35, (byte) 0x78, (byte) 0xb3, (byte) 0xe4, (byte) 0x17, (byte) 0xcb,
				(byte) 0x71, (byte) 0xce, (byte) 0x64, (byte) 0x6e, (byte) 0xfd, (byte) 0x08, (byte) 0x19, (byte) 0xdd, (byte) 0x8c,
				(byte) 0x08, (byte) 0x8d, (byte) 0xe1, (byte) 0xbd },
				(byte[]) (FunctionUtil.parseExpr("hash(\"sha384\", binary(\"hello world\"))").eval(null)));
		assertArrayEquals(new byte[] { (byte) 0x30, (byte) 0x9e, (byte) 0xcc, (byte) 0x48, (byte) 0x9c, (byte) 0x12, (byte) 0xd6, (byte) 0xeb,
				(byte) 0x4c, (byte) 0xc4, (byte) 0x0f, (byte) 0x50, (byte) 0xc9, (byte) 0x02, (byte) 0xf2, (byte) 0xb4, (byte) 0xd0,
				(byte) 0xed, (byte) 0x77, (byte) 0xee, (byte) 0x51, (byte) 0x1a, (byte) 0x7c, (byte) 0x7a, (byte) 0x9b, (byte) 0xcd,
				(byte) 0x3c, (byte) 0xa8, (byte) 0x6d, (byte) 0x4c, (byte) 0xd8, (byte) 0x6f, (byte) 0x98, (byte) 0x9d, (byte) 0xd3,
				(byte) 0x5b, (byte) 0xc5, (byte) 0xff, (byte) 0x49, (byte) 0x96, (byte) 0x70, (byte) 0xda, (byte) 0x34, (byte) 0x25,
				(byte) 0x5b, (byte) 0x45, (byte) 0xb0, (byte) 0xcf, (byte) 0xd8, (byte) 0x30, (byte) 0xe8, (byte) 0x1f, (byte) 0x60,
				(byte) 0x5d, (byte) 0xcf, (byte) 0x7d, (byte) 0xc5, (byte) 0x54, (byte) 0x2e, (byte) 0x93, (byte) 0xae, (byte) 0x9c,
				(byte) 0xd7, (byte) 0x6f },
				(byte[]) (FunctionUtil.parseExpr("hash(\"sha512\", binary(\"hello world\"))").eval(null)));
		assertNull(FunctionUtil.parseExpr("hash(\"md5\", \"hello world\")").eval(null));
		assertNull(FunctionUtil.parseExpr("hash(\"sha1\", int(\"invalid\"))").eval(null));
		assertNull(FunctionUtil.parseExpr("hash(\"sha1\", 1234)").eval(null));
	}
}
