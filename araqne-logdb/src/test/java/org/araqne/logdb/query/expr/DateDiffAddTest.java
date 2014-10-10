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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.araqne.logdb.QueryParseException;
import org.junit.Before;
import org.junit.Test;
/**
 * 
 * @author kyun
 *
 */
public class DateDiffAddTest {
	private String format =  "yyyy-MM-dd HH:mm:ss";
	private SimpleDateFormat sdf = new SimpleDateFormat(format);
	private Date startDate; 
	private Date endDate;
	
	@Before
	public void setup() throws ParseException {
		startDate = sdf.parse("2014-09-18 13:55:00");
		endDate = sdf.parse("2015-09-18 13:55:00");
	}

	@Test
	public void testDateDiff(){
		DateDiff dateDiff = null;
		
		//day
		dateDiff = new DateDiff(null, expr(date( sdf.format(startDate), format) , date( sdf.format(endDate), format), "day"));
		assertEquals(365L, dateDiff.eval(null));

		//mon
		dateDiff = new DateDiff(null, expr(date( sdf.format(startDate), format) , date( sdf.format(endDate), format), "mon"));
		assertEquals(12L, dateDiff.eval(null));
		
		//year
		dateDiff = new DateDiff(null, expr(date( sdf.format(startDate), format) , date( sdf.format(endDate), format), "year"));
		assertEquals(1L, dateDiff.eval(null));
		
		//hour
		dateDiff = new DateDiff(null, expr(date( sdf.format(startDate), format) , date( sdf.format(endDate), format), "hour"));
		assertEquals(365L * 24L, dateDiff.eval(null));
		
		//min
		dateDiff = new DateDiff(null, expr(date( sdf.format(startDate), format) , date( sdf.format(endDate), format), "min"));
		assertEquals(365L * 24L * 60L, dateDiff.eval(null));
		
		//sec
		dateDiff = new DateDiff(null, expr(date( sdf.format(startDate), format) , date( sdf.format(endDate), format), "sec"));
		assertEquals(365L * 24L * 60L * 60L, dateDiff.eval(null));
		
		//msec
		dateDiff = new DateDiff(null, expr(date( sdf.format(startDate), format) , date( sdf.format(endDate), format), "msec"));
		assertEquals(365L * 24L * 60L * 60L * 1000L, dateDiff.eval(null));
	}

	@Test
	public void testError90630(){
		
		//args size 2
		try {
			new DateDiff(null, expr(null, null ));
			fail();
		} catch (QueryParseException e) {
			if (e.isDebugMode()) {
				System.out.println(e.getMessage());
			}
			assertEquals("90630", e.getType());
		}
		
		//args size 4
		try {
			new DateDiff(null, expr(null, null, "sec", 1));
			fail();
		} catch (QueryParseException e) {
			if (e.isDebugMode()) {
				System.out.println(e.getMessage());
			}
			assertEquals("90630", e.getType());
		}
	}

	@Test
	public void testError90631(){
		try {
			new DateDiff(null, expr(null, null, "seconds"));
			fail();
		} catch (QueryParseException e) {
			if (e.isDebugMode()) {
				System.out.println(e.getMessage());
			}
			assertEquals("90631", e.getType());
			assertEquals("seconds", e.getParams().get("field"));
		}
	}

	private ToDate date(String date, String format){
		List<Expression> expr = new ArrayList<Expression>();

		expr.add(new StringConstant(date));
		expr.add(new StringConstant(format));

		return new ToDate(null, expr);
	}

	private List<Expression> expr(ToDate startDate, ToDate endDate, Object... object){
		List<Expression> expr = new ArrayList<Expression>();

		expr.add(startDate);
		expr.add(endDate);
		for(Object o: object){
			if(o instanceof String)
				expr.add(new StringConstant((String)o));
			else if(o instanceof Number)
				expr.add(new NumberConstant((Number)o));
		}
		
		return expr;
	}
}
