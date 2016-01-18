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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.api.MapTypeHint;

public class LastState {
	private String loggerName;
	private int interval;

	/**
	 * HHmm format
	 * 
	 * @since 3.4.0
	 */
	private String startTime;

	/**
	 * HHmm format
	 * 
	 * @since 3.4.0
	 */
	private String endTime;

	/**
	 * started/stopped by user request
	 * 
	 * @since 3.4.0
	 */
	private boolean isEnabled;

	private boolean isRunning;
	private boolean isPending;
	private long logCount;
	private long dropCount;
	private long logVolume;
	private long dropVolume;
	private Date lastLogDate;

	/***
	 * check newer state between HA nodes
	 */
	private long updateCount;

	@MapTypeHint({ String.class, Object.class })
	private Map<String, Object> properties = new HashMap<String, Object>();

	@SuppressWarnings("unchecked")
	public static LastState cloneState(LastState old) {
		LastState clone = new LastState();
		clone.setLoggerName(old.getLoggerName());
		clone.setInterval(old.getInterval());
		clone.setStartTime(old.getStartTime());
		clone.setEndTime(old.getEndTime());
		clone.setLogCount(old.getLogCount());
		clone.setDropCount(old.getDropCount());
		clone.setPending(old.isPending());
		clone.setEnabled(old.isEnabled());
		clone.setRunning(old.isRunning());
		clone.setLastLogDate(old.getLastLogDate());
		clone.setUpdateCount(old.getUpdateCount());
		clone.setProperties((Map<String, Object>) deepCopy(old.getProperties()));
		clone.setLogVolume(old.getLogVolume());
		clone.setDropVolume(old.getDropVolume());

		return clone;
	}

	@SuppressWarnings("unchecked")
	private static Object deepCopy(Object o) {
		if (o == null)
			return null;

		if (o instanceof Map) {
			Map<String, Object> dup = new HashMap<String, Object>();
			Map<String, Object> m = (Map<String, Object>) o;
			for (String key : m.keySet())
				dup.put(key, deepCopy(m.get(key)));
			return dup;
		} else if (o instanceof List) {
			List<Object> dup = new ArrayList<Object>();
			List<Object> l = (List<Object>) o;
			for (Object el : l)
				dup.add(deepCopy(el));
			return dup;
		} else
			return o;
	}

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

	public String getStartTime() {
		return startTime;
	}

	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}

	public String getEndTime() {
		return endTime;
	}

	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}

	public boolean isEnabled() {
		return isEnabled;
	}

	public void setEnabled(boolean isEnabled) {
		this.isEnabled = isEnabled;
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

	public long getUpdateCount() {
		return updateCount;
	}

	public void setUpdateCount(long updateCount) {
		this.updateCount = updateCount;
	}

	public Map<String, Object> getProperties() {
		return properties;
	}

	public void setProperties(Map<String, Object> properties) {
		this.properties = properties;
	}

	public long getLogVolume() {
		return logVolume;
	}

	public void setLogVolume(long logVolume) {
		this.logVolume = logVolume;
	}

	public long getDropVolume() {
		return dropVolume;
	}

	public void setDropVolume(long dropVolume) {
		this.dropVolume = dropVolume;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (dropCount ^ (dropCount >>> 32));
		result = prime * result + ((endTime == null) ? 0 : endTime.hashCode());
		result = prime * result + interval;
		result = prime * result + (isEnabled ? 1231 : 1237);
		result = prime * result + (isPending ? 1231 : 1237);
		result = prime * result + (isRunning ? 1231 : 1237);
		result = prime * result + ((lastLogDate == null) ? 0 : lastLogDate.hashCode());
		result = prime * result + (int) (logCount ^ (logCount >>> 32));
		result = prime * result + ((loggerName == null) ? 0 : loggerName.hashCode());
		result = prime * result + ((properties == null) ? 0 : properties.hashCode());
		result = prime * result + ((startTime == null) ? 0 : startTime.hashCode());
		// do not compare update count
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
		if (logVolume != other.logVolume)
			return false;
		if (dropVolume != other.dropVolume)
			return false;
		if (endTime == null) {
			if (other.endTime != null)
				return false;
		} else if (!endTime.equals(other.endTime))
			return false;
		if (interval != other.interval)
			return false;
		if (isEnabled != other.isEnabled)
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
		if (startTime == null) {
			if (other.startTime != null)
				return false;
		} else if (!startTime.equals(other.startTime))
			return false;

		// do not compare update count
		return true;
	}
}
