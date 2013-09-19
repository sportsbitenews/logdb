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
package org.araqne.logdb.impl;

import java.util.List;
import java.util.Map;

import org.araqne.log.api.LogParser;
import org.araqne.log.api.LogParserInput;
import org.araqne.log.api.LogParserOutput;
import org.araqne.logdb.LogMap;
import org.araqne.logdb.LogQuery;
import org.araqne.logdb.LogQueryCommand;

public class QueryLogParser extends LogQueryCommand implements LogParser {
	private String queryString;
	private LogQueryCommand first;
	private Map<String, Object> last;

	public QueryLogParser(LogQuery q) {
		queryString = q.getQueryString();
		List<LogQueryCommand> commands = q.getCommands();
		first = commands.get(0);
		commands.add(this);

		for (int i = commands.size() - 2; i >= 0; i--)
			commands.get(i).setNextCommand(commands.get(i + 1));
	}

	@Override
	public int getVersion() {
		return 1;
	}

	@Override
	public LogParserOutput parse(LogParserInput input) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		first.push(new LogMap(params));
		Map<String, Object> m = last;
		last = null;
		return m;
	}

	@Override
	public void push(LogMap m) {
		last = m.map();
	}

	@Override
	public boolean isReducer() {
		return false;
	}

	@Override
	public String toString() {
		return "query log parser: " + queryString;
	}
}
