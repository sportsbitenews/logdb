/*
 * Copyright 2013 Future Systems
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
package org.araqne.logdb.query.parser;

import java.util.Map;

import org.araqne.logdb.LogQueryCommand;
import org.araqne.logdb.LogQueryCommandParser;
import org.araqne.logdb.LogQueryContext;
import org.araqne.logdb.LogQueryScript;
import org.araqne.logdb.LogQueryScriptRegistry;
import org.araqne.logdb.query.command.Script;
import org.osgi.framework.BundleContext;

public class ScriptParser implements LogQueryCommandParser {
	private BundleContext bc;
	private LogQueryScriptRegistry scriptRegistry;

	public ScriptParser(BundleContext bc, LogQueryScriptRegistry scriptRegistry) {
		this.bc = bc;
		this.scriptRegistry = scriptRegistry;
	}

	@Override
	public String getCommandName() {
		return "script";
	}

	@SuppressWarnings("unchecked")
	@Override
	public LogQueryCommand parse(LogQueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(commandString, getCommandName().length());
		String name = commandString.substring(r.next);

		LogQueryScript script = scriptRegistry.newScript("localhost", name, null);
		if (script == null)
			throw new IllegalArgumentException("log script not found: " + name);

		Map<String, String> params = (Map<String, String>) r.value;
		script.init(params);
		return new Script(bc, name, params, script);
	}
}