/*
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
package org.araqne.logstorage.file;

import java.util.Date;

import org.araqne.logstorage.CallbackSet;
import org.araqne.storage.api.FilePath;

/**
 * @since 2.2.0
 * @author xeraph
 * 
 */
public class LogWriterConfigV3o {
	private String tableName;
	private Date day;
	private FilePath indexPath;
	private FilePath dataPath;
	private LogStatsListener listener;
	private int flushCount;
	private int level = 3;
	private String compression = "deflater";
	private CallbackSet callbackSet;

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

	public FilePath getIndexPath() {
		return indexPath;
	}

	public void setIndexPath(FilePath indexPath) {
		this.indexPath = indexPath;
	}

	public FilePath getDataPath() {
		return dataPath;
	}

	public void setDataPath(FilePath dataPath) {
		this.dataPath = dataPath;
	}

	public LogStatsListener getListener() {
		return listener;
	}

	public void setListener(LogStatsListener listener) {
		this.listener = listener;
	}

	public int getFlushCount() {
		return flushCount;
	}

	public void setFlushCount(int flushCount) {
		this.flushCount = flushCount;
	}

	public int getLevel() {
		if (compression == null || compression.equals("none"))
			return 0;
		return level;
	}

	public void setLevel(int level) {
		this.level = level;
	}

	public String getCompression() {
		return compression;
	}

	public void setCompression(String compression) {
		this.compression = compression;
	}

	public void setCallbackSet(CallbackSet callbackSet) {
		this.callbackSet = callbackSet;
	}

	public CallbackSet getCallbackSet() {
		return this.callbackSet;
	}
}
