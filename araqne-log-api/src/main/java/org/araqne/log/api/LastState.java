/*
 * Copyright 2013 Eediom Inc.
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

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.araqne.api.MapTypeHint;

public class LastState {
	private String loggerName;
	private int interval;
	private boolean isRunning;
	private boolean isPending;
	private long logCount;
	private long dropCount;
	private Date lastLogDate;

	@MapTypeHint({ String.class, Object.class })
	private Map<String, Object> properties = new HashMap<String, Object>();

	public String getLoggerName() {
		return loggerName;
	}

	public void setLoggerName(String loggerName) {
		this.loggerName = loggerName;
	}

	public int getInterval() {
		return interval;
	}

	public void setInterval(int interval) {
		this.interval = interval;
	}

	public boolean isRunning() {
		return isRunning;
	}

	public void setRunning(boolean isRunning) {
		this.isRunning = isRunning;
	}

	public boolean isPending() {
		return isPending;
	}

	public void setPending(boolean isPending) {
		this.isPending = isPending;
	}

	public long getLogCount() {
		return logCount;
	}

	public void setLogCount(long logCount) {
		this.logCount = logCount;
	}

	public long getDropCount() {
		return dropCount;
	}

	public void setDropCount(long dropCount) {
		this.dropCount = dropCount;
	}

	public Date getLastLogDate() {
		return lastLogDate;
	}

	public void setLastLogDate(Date lastLogDate) {
		this.lastLogDate = lastLogDate;
	}

	public Map<String, Object> getProperties() {
		return properties;
	}

	public void setProperties(Map<String, Object> properties) {
		this.properties = properties;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (dropCount ^ (dropCount >>> 32));
		result = prime * result + interval;
		result = prime * result + (isPending ? 1231 : 1237);
		result = prime * result + (isRunning ? 1231 : 1237);
		result = prime * result + ((lastLogDate == null) ? 0 : lastLogDate.hashCode());
		result = prime * result + (int) (logCount ^ (logCount >>> 32));
		result = prime * result + ((loggerName == null) ? 0 : loggerName.hashCode());
		result = prime * result + ((properties == null) ? 0 : properties.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		LastState other = (LastState) obj;
		if (dropCount != other.dropCount)
			return false;
		if (interval != other.interval)
			return false;
		if (isPending != other.isPending)
			return false;
		if (isRunning != other.isRunning)
			return false;
		if (lastLogDate == null) {
			if (other.lastLogDate != null)
				return false;
		} else if (!lastLogDate.equals(other.lastLogDate))
			return false;
		if (logCount != other.logCount)
			return false;
		if (loggerName == null) {
			if (other.loggerName != null)
				return false;
		} else if (!loggerName.equals(other.loggerName))
			return false;
		if (properties == null) {
			if (other.properties != null)
				return false;
		} else if (!properties.equals(other.properties))
			return false;
		return true;
	}
}
