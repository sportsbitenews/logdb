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
import java.util.Calendar;
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
public class DateAddTest {
	private String format =  "yyyy-MM-dd HH:mm:ss";
	private SimpleDateFormat sdf = new SimpleDateFormat(format);
	private Calendar cal = Calendar.getInstance();
	private Date date; 

	@Before
	public void setup() throws ParseException {
		date = sdf.parse("2014-09-18 13:55:00");
	}

	@Test
	public void testDateAdd(){
		DateAdd dateAdd = null;

		//day
		dateAdd = new DateAdd(null, expr(date( sdf.format(date), format) , "day", 1));
		cal.setTime(date);
		cal.add(Calendar.DAY_OF_MONTH, 1);
		assertEquals(cal.getTime(), (Date)dateAdd.eval(null));

		//mon
		dateAdd = new DateAdd(null, expr(date( sdf.format(date), format) , "mon", 1));
		cal.setTime(date);
		cal.add(Calendar.MONTH, 1);
		assertEquals(cal.getTime(), (Date)dateAdd.eval(null));

		//year
		dateAdd = new DateAdd(null, expr(date( sdf.format(date), format) , "year", 1));
		cal.setTime(date);
		cal.add(Calendar.YEAR, 1);
		assertEquals(cal.getTime(), (Date)dateAdd.eval(null));

		//hour
		dateAdd = new DateAdd(null, expr(date( sdf.format(date), format) , "hour", 1));
		cal.setTime(date);
		cal.add(Calendar.HOUR_OF_DAY, 1);
		assertEquals(cal.getTime(), (Date)dateAdd.eval(null));

		//min
		dateAdd = new DateAdd(null, expr(date( sdf.format(date), format) , "min", 1));
		cal.setTime(date);
		cal.add(Calendar.MINUTE, 1);
		assertEquals(cal.getTime(), (Date)dateAdd.eval(null));

		//sec
		dateAdd = new DateAdd(null, expr(date( sdf.format(date), format) , "sec", 1));
		cal.setTime(date);
		cal.add(Calendar.SECOND, 1);
		assertEquals(cal.getTime(), (Date)dateAdd.eval(null));

	}

	@Test
	public void testError90620(){
		
		//args size 1
		try {
			new DateAdd(null,  expr(date( sdf.format(date), format)));
			fail();
		} catch (QueryParseException e) {
			if (e.isDebugMode()) {
				System.out.println(e.getMessage());
			}
			assertEquals("90620", e.getType());
		}
		
		//args size 2
		try {
			new DateAdd(null,  expr(date( sdf.format(date), format) , "sec"));
			fail();
		} catch (QueryParseException e) {
			if (e.isDebugMode()) {
				System.out.println(e.getMessage());
			}
			assertEquals("90620", e.getType());
		}
		
		//args size 4
		try {
			new DateAdd(null,  expr(date( sdf.format(date), format) , "sec", 1, 2));
			fail();
		} catch (QueryParseException e) {
			if (e.isDebugMode()) {
				System.out.println(e.getMessage());
			}
			assertEquals("90620", e.getType());
		}
	}


	@Test
	public void testError90621(){
		try {
			new DateAdd(null, expr(date( sdf.format(date), format) , "seconds", 1));
			fail();
		} catch (QueryParseException e) {
			if (e.isDebugMode()) {
				System.out.println(e.getMessage());
			}
			assertEquals("90621", e.getType());
			assertEquals("seconds", e.getParams().get("field"));
		}
	}

	@Test
	public void testError90622(){
		try {
			new DateAdd(null, expr(date( sdf.format(date), format) , "sec", 1.2));
			fail();
		} catch (QueryParseException e) {
			if (e.isDebugMode()) {
				System.out.println(e.getMessage());
			}
			assertEquals("90622", e.getType());
			assertEquals("1.2", e.getParams().get("time"));
		}
		
		try {
			new DateAdd(null, expr(date( sdf.format(date), format) , "sec", "now().sec"));
			fail();
		} catch (QueryParseException e) {
			if (e.isDebugMode()) {
				System.out.println(e.getMessage());
			}
			assertEquals("90622", e.getType());
			assertEquals("now().sec", e.getParams().get("time"));
		}
	}

	private ToDate date(String date, String format){
		List<Expression> expr = new ArrayList<Expression>();

		expr.add(new StringConstant(date));
		expr.add(new StringConstant(format));

		return new ToDate(null, expr);
	}

	private List<Expression> expr(ToDate date, Object... object){
		List<Expression> expr = new ArrayList<Expression>();

		expr.add(date);
		for(Object o: object){
			if(o instanceof String)
				expr.add(new StringConstant((String)o));
			else if(o instanceof Number)
				expr.add(new NumberConstant((Number)o));
		}

		return expr;
	}
}
