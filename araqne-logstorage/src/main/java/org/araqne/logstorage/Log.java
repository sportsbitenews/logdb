/*
 * Copyright 2010 NCHOVY
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
package org.araqne.logstorage;

import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.Map;
import java.util.Set;

import org.araqne.logstorage.engine.DateUtil;

public class Log implements Comparable<Log> {
	private String tableName;
	private Date date;
	private Date day;
	private long id;
	private Map<String, Object> data;
	private Set<String> indexTokens;

	/**
	 * @since 1.12.3
	 */
	private long binaryLength;

	private Log() {
	}

	public Log(String tableName, Date date, Map<String, Object> data) {
		this(tableName, date, 0, data);
	}

	public Log(String tableName, Date date, long id, Map<String, Object> data) {
		this.tableName = tableName;
		this.date = date;
		this.day = DateUtil.getDay(date);
		this.id = id;
		this.data = data;
	}

	public Log(String tableName, Date date, Date day, long id, Map<String, Object> data) {
		this.tableName = tableName;
		this.date = date;
		this.day = day == null ? DateUtil.getDay(date) : day;
		this.id = id;
		this.data = data;
	}

	@Deprecated
	public Log(String tableName, Date date, long id, Map<String, Object> data, Set<String> indexTokens) {
		this(tableName, date, id, data);
		this.indexTokens = indexTokens;
	}

	public String getTableName() {
		return tableName;
	}

	public Date getDay() {
		return day;
	}

	public Date getDate() {
		return date;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public Map<String, Object> getData() {
		return data;
	}

	@Deprecated
	public Set<String> getIndexTokens() {
		return indexTokens;
	}

	public long getBinaryLength() {
		return binaryLength;
	}

	public void setBinaryLength(long binaryLength) {
		this.binaryLength = binaryLength;
	}

	public Log shallowCopy() {
		Log copy = new Log();
		copy.tableName = tableName;
		copy.date = date;
		copy.day = day;
		copy.id = id;
		copy.data = data;
		copy.indexTokens = indexTokens;
		return copy;
	}

	@Override
	public String toString() {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		StringBuilder sb = new StringBuilder();
		int i = 0;
		for (String key : data.keySet()) {
			if (i != 0)
				sb.append(", ");
			sb.append(key);
			sb.append("=");
			sb.append(data.get(key));
			i++;
		}

		return String.format("date=%s, id=%d, %s", dateFormat.format(date), id, sb.toString());
	}

	public static class IDOnlyComparator implements Comparator<Log> {
		@Override
		public int compare(Log o1, Log o2) {
			return Long.signum(o1.id - o2.id);
		}
	}

	@Override
	public int compareTo(Log o) {
		if (this.tableName.equals(o.tableName)) {
			if (this.day.equals(o.day)) {
				return Long.signum(this.id - o.id);
			} else {
				return this.day.compareTo(o.day);
			}
		} else {
			return this.tableName.compareTo(o.tableName);
		}
	}
}
