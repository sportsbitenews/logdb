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
package org.araqne.log.api;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Test;

public class TimeRangeTest {
	@Test
	public void testRanges() {
		// 00:00 to 06:00
		TimeRange range = new TimeRange("00:00", "06:00");
		assertFalse(range.isInRange(t("2014-09-06 23:59")));
		assertTrue(range.isInRange(t("2014-09-07 00:00")));
		assertTrue(range.isInRange(t("2014-09-07 06:00")));
		assertFalse(range.isInRange(t("2014-09-07 07:00")));

		// 22:00 to 04:00 (night run)
		TimeRange range2 = new TimeRange("22:00", "04:00");
		assertFalse(range2.isInRange(t("2014-09-06 21:00")));
		assertTrue(range2.isInRange(t("2014-09-06 22:00")));
		assertTrue(range2.isInRange(t("2014-09-07 23:59")));
		assertTrue(range2.isInRange(t("2014-09-07 00:00")));
		assertTrue(range2.isInRange(t("2014-09-07 04:00")));
		assertFalse(range2.isInRange(t("2014-09-07 04:01")));
	}

	private Date t(String s) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		return df.parse(s, new ParsePosition(0));
	}
}
