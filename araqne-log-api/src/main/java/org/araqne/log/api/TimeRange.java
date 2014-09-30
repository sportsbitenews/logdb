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

import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeRange {
	private String startTime;
	private String endTime;

	public TimeRange(String startTime, String endTime) {
		if (startTime == null)
			throw new IllegalArgumentException("start time should be not null");

		if (endTime == null)
			throw new IllegalArgumentException("end time should be not null");

		this.startTime = startTime;
		this.endTime = endTime;
	}

	public boolean isInRange(Date time) {
		if (time == null)
			throw new IllegalArgumentException("time should be not null");

		SimpleDateFormat df = new SimpleDateFormat("HH:mm");
		String now = df.format(time);

		if (startTime.compareTo(endTime) <= 0) {
			return startTime.compareTo(now) <= 0 && now.compareTo(endTime) <= 0;
		} else {
			return (startTime.compareTo(now) <= 0 && now.compareTo("24:00") <= 0)
					|| ("00:00".compareTo(now) <= 0 && now.compareTo(endTime) <= 0);
		}
	}

	public String getStartTime() {
		return startTime;
	}

	public String getEndTime() {
		return endTime;
	}

	@Override
	public String toString() {
		return startTime + " ~ " + endTime;
	}
}
