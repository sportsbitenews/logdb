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

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.junit.Test;

public class DateFunctionTest {
	//TODO : dateadd, epoch
	@Test
	public void testDateManual() throws ParseException {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
		assertEquals(format.parse("2013-06-10 00:30:55.978"), FunctionUtil.parseExpr("date(\"2013-06-10 00:30:55.978\", \"yyyy-MM-dd HH:mm:ss.SSS\")").eval(null));
	}
	
	@Test
	public void testDateDiffManual() {
		assertEquals(1L, FunctionUtil.parseExpr("datediff(date(\"2013-09-29\", \"yyyy-MM-dd\"), date(\"2014-09-29\", \"yyyy-MM-dd\"), \"year\")").eval(null));
		assertEquals(12L, FunctionUtil.parseExpr("datediff(date(\"2013-09-29\", \"yyyy-MM-dd\"), date(\"2014-09-29\", \"yyyy-MM-dd\"), \"mon\")").eval(null));
		assertEquals(365L, FunctionUtil.parseExpr("datediff(date(\"2013-09-29\", \"yyyy-MM-dd\"), date(\"2014-09-29\", \"yyyy-MM-dd\"), \"day\")").eval(null));
		assertEquals(8760L, FunctionUtil.parseExpr("datediff(date(\"2013-09-29\", \"yyyy-MM-dd\"), date(\"2014-09-29\", \"yyyy-MM-dd\"), \"hour\")").eval(null));
		assertEquals(525600L, FunctionUtil.parseExpr("datediff(date(\"2013-09-29\", \"yyyy-MM-dd\"), date(\"2014-09-29\", \"yyyy-MM-dd\"), \"min\")").eval(null));
		assertEquals(31536000L, FunctionUtil.parseExpr("datediff(date(\"2013-09-29\", \"yyyy-MM-dd\"), date(\"2014-09-29\", \"yyyy-MM-dd\"), \"sec\")").eval(null));
		assertEquals(31536000000L, FunctionUtil.parseExpr("datediff(date(\"2013-09-29\", \"yyyy-MM-dd\"), date(\"2014-09-29\", \"yyyy-MM-dd\"), \"msec\")").eval(null));
		assertNull(FunctionUtil.parseExpr("datediff(int(\"invalid\"), date(\"2014-09-29\", \"yyyy-MM-dd\"), \"sec\")").eval(null));
		assertNull(FunctionUtil.parseExpr("datediff(date(\"2013-09-29\", \"yyyy-MM-dd\"), int(\"invalid\"), \"min\")").eval(null));
		assertNull(FunctionUtil.parseExpr("datediff(\"invalid\", date(\"2014-09-29\", \"yyyy-MM-dd\"), \"min\")").eval(null));
	}
	
	@Test
	public void testDateTruncManual() throws ParseException {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");
		assertEquals(format.parse("2014-07-14 11:13:00+0900"), FunctionUtil.parseExpr("datetrunc(date(\"2014-07-14 11:13:23\", \"yyyy-MM-dd HH:mm:ss\"),\"1m\")").eval(null));
		assertEquals(format.parse("2014-07-14 11:10:00+0900"), FunctionUtil.parseExpr("datetrunc(date(\"2014-07-14 11:13:23\", \"yyyy-MM-dd HH:mm:ss\"),\"5m\")").eval(null));
		assertEquals(format.parse("2014-07-01 00:00:00+0900"), FunctionUtil.parseExpr("datetrunc(date(\"2014-07-14 11:13:23\", \"yyyy-MM-dd HH:mm:ss\"),\"1mon\")").eval(null));
	}

}
