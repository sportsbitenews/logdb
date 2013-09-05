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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @since 2.4.6
 * @author xeraph
 * 
 */
public class LineLog implements Log {

	private Date date;
	private String fullName;
	private String line;
	private Map<String, Object> data;

	public LineLog(Date date, String fullName, String line) {
		this.date = date;
		this.fullName = fullName;
		this.line = line;
		this.data = new HashMap<String, Object>();
		this.data.put("line", line);
	}

	@Override
	public Date getDate() {
		return new Date();
	}

	@Override
	public String getLoggerName() {
		return fullName;
	}

	@Override
	public String getMessage() {
		return line;
	}

	@Override
	public Map<String, Object> getParams() {
		return data;
	}

	@Override
	public String toString() {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return String.format("date=%s, logger=%s, line=%s", dateFormat.format(date), fullName, line);
	}
}
