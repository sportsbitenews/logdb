/**
 * Copyright 2015 Eediom Inc.
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
package org.araqne.logdb.nashorn.impl;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.nashorn.NashornQueryScript;
import org.araqne.logdb.nashorn.NashornQueryScriptRegistry;

@Component(name = "logdb-nashorn-query-parser")
public class NashornQueryParser extends AbstractQueryCommandParser {
	@Requires
	private NashornQueryScriptRegistry scriptRegistry;

	@Requires
	private QueryParserService queryParserService;

	public NashornQueryParser() {
		setDescriptions("Run user defind javascript query.", "사용자 정의 자바스크립트 쿼리를 실행합니다.");
	}

	@Override
	public String getCommandName() {
		return "javascript";
	}

	@Validate
	public void start() {
		queryParserService.addCommandParser(this);
	}

	@Invalidate
	public void stop() {
		if (queryParserService != null)
			queryParserService.removeCommandParser(this);
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		try {
			String scriptName = commandString.substring(getCommandName().length()).trim();
			NashornQueryScript script = scriptRegistry.newScript(scriptName);
			return new NashornQueryCommand(script);
		} catch (Throwable t) {
			throw new QueryParseException("javascript-failure", -1, t.getMessage());
		}

	}

}
