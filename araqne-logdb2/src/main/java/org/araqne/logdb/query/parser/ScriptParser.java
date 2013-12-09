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

import java.util.ArrayList;
import java.util.Map;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryCommandParser;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryScript;
import org.araqne.logdb.QueryScriptRegistry;
import org.araqne.logdb.query.command.Script;
import org.osgi.framework.BundleContext;

public class ScriptParser implements QueryCommandParser {
	private BundleContext bc;
	private QueryScriptRegistry scriptRegistry;

	public ScriptParser(BundleContext bc, QueryScriptRegistry scriptRegistry) {
		this.bc = bc;
		this.scriptRegistry = scriptRegistry;
	}

	@Override
	public String getCommandName() {
		return "script";
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(), new ArrayList<String>());
		String name = commandString.substring(r.next);

		QueryScript script = scriptRegistry.newScript("localhost", name, null);
		if (script == null)
			throw new IllegalArgumentException("log script not found: " + name);

		Map<String, String> params = (Map<String, String>) r.value;
		script.init(params);
		return new Script(bc, name, params, script);
	}
}