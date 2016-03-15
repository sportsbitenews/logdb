/**
 * Copyright 2016 Eediom Inc.
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

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class ScanPeriodMatcher {
	private final SimpleDateFormat dateFormat;
	private final String timeZone;
	private final int scanDays;

	// assign current year to date
	private Calendar yearModifier;

	public ScanPeriodMatcher(SimpleDateFormat dateFormat, String timeZone, int scanDays) {
		this.dateFormat = dateFormat;
		this.timeZone = timeZone;
		this.scanDays = scanDays;

		if (timeZone != null) {
			if (TimeZoneMappings.getTimeZone(timeZone) != null)
				timeZone = (String) TimeZoneMappings.getTimeZone(timeZone);

			dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
		}

		if (dateFormat != null && !dateFormat.toPattern().contains("yyyy")) {
			yearModifier = Calendar.getInstance();
			if (timeZone != null)
				yearModifier.setTimeZone(TimeZone.getTimeZone(timeZone));
		}
	}

	public boolean matches(long baseTime, String dateFromPath) {
		if (dateFormat == null || scanDays < 1)
			return true;

		// if config is not valid, match all
		Date d = dateFormat.parse(dateFromPath, new ParsePosition(0));
		if (d == null)
			return true;

		return baseTime - d.getTime() <= scanDays * 86400000L;
	}

	@Override
	public String toString() {
		return "date format=" + dateFormat + ", timezone=" + timeZone + ", scan days=" + scanDays;
	}
}
