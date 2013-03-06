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
package org.araqne.logdb.query.command;

import java.util.List;

import org.araqne.logdb.LogMap;
import org.araqne.logdb.LogQueryCommand;

public class Json extends LogQueryCommand {

	private List<LogMap> logs;

	public Json(List<LogMap> logs) {
		this.logs = logs;
	}

	@Override
	public void push(LogMap m) {
	}

	@Override
	public void start() {
		status = Status.Running;
		for (LogMap log : logs)
			write(log);
		eof();
	}

	@Override
	public boolean isReducer() {
		return false;
	}
}
