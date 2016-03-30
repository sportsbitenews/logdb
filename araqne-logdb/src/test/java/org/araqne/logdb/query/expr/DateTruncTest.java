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
import org.araqne.logdb.Row;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * 
 * @author kyun
 *
 */
public class DateTruncTest {
	private String format = "yyyy-MM-dd HH:mm:ss";
	private SimpleDateFormat sdf = new SimpleDateFormat(format);
	private Date date;

	@Before
	public void setup() throws ParseException {
		date = sdf.parse("2014-09-18 13:55:10");

	}

	/**
	 * Locale 문제로 KST가 아닌 지역에서 빌드를 하면 에러가 발생하므로 임시로 Ignore 함 (issue #651 참조)
	 * 
	 * @author kyun
	 */
	@Ignore
	@Test
	public void testDateTrunc() throws ParseException {
		DateTrunc dateTrunc = null;

		dateTrunc = new DateTrunc(null, expr(date(sdf.format(date), format), "1m"));
		assertEquals(sdf.parse("2014-09-18 13:55:00"), dateTrunc.eval((Row) null));

		dateTrunc = new DateTrunc(null, expr(date(sdf.format(date), format), "10m"));
		assertEquals(sdf.parse("2014-09-18 13:50:00"), dateTrunc.eval((Row) null));

		dateTrunc = new DateTrunc(null, expr(date(sdf.format(date), format), "1mon"));
		assertEquals(sdf.parse("2014-09-01 00:00:00"), dateTrunc.eval((Row) null));
	}

	@Test
	public void testError90640() {

		// args size 1
		try {
			new DateTrunc(null, expr(null));
			fail();
		} catch (QueryParseException e) {
			if (e.isDebugMode()) {
				System.out.println(e.getMessage());
			}
			assertEquals("90640", e.getType());
		}

	}

	private ToDate date(String date, String format) {
		List<Expression> expr = new ArrayList<Expression>();

		expr.add(new StringConstant(date));
		expr.add(new StringConstant(format));

		return new ToDate(null, expr);
	}

	private List<Expression> expr(ToDate date, Object... object) {
		List<Expression> expr = new ArrayList<Expression>();

		expr.add(date);
		for (Object o : object) {
			if (o instanceof String)
				expr.add(new StringConstant((String) o));
			else if (o instanceof Number)
				expr.add(new NumberConstant((Number) o));
		}

		return expr;
	}
}
