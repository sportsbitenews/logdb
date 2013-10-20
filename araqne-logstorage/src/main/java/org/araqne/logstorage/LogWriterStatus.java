/*
 * Copyright 2012 Future Systems
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
import java.util.Date;

public class LogWriterStatus {
	private String tableName;
	private Date day;
	private Date lastWrite;
	private int bufferSize;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public Date getDay() {
		return day;
	}

	public void setDay(Date day) {
		this.day = day;
	}

	public Date getLastWrite() {
		return lastWrite;
	}

	public void setLastWrite(Date lastWrite) {
		this.lastWrite = lastWrite;
	}

	/**
	 * @since 2.2.6
	 */
	public int getBufferSize() {
		return bufferSize;
	}

	public void setBufferSize(int size) {
		this.bufferSize = size;
	}

	@Override
	public String toString() {
		SimpleDateFormat dayFormat = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return "table=" + tableName + ", day=" + dayFormat.format(day) + ", buffer size=" + bufferSize
				+ ", last write=" + dateFormat.format(lastWrite);
	}
}
