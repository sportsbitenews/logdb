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

import java.text.ParseException;
import java.util.Date;
import java.util.Map;

public class LogParserBugException extends ParseException {
	private static final long serialVersionUID = 1L;
	public Throwable cause;
	public String tableName;
	public long id;
	public Date date;
	public Map<String, Object> logMap;

	public LogParserBugException(Throwable cause, String tableName, long id, Date date, Map<String, Object> logMap) {
		super("parser bug", -1);
		this.cause = cause;
		this.tableName = tableName;
		this.id = id;
		this.date = date;
		this.logMap = logMap;
	}
}
