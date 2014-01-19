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
package org.araqne.logdb.groovy.impl;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryCommandParser;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.groovy.GroovyQueryScript;
import org.araqne.logdb.groovy.GroovyQueryScriptRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name = "groovy-query-parser")
public class GroovyQueryParser implements QueryCommandParser {
	private final Logger slog = LoggerFactory.getLogger(GroovyQueryParser.class);

	@Requires
	private GroovyQueryScriptRegistry scriptRegistry;

	@Requires
	private QueryParserService queryParserService;

	@Override
	public String getCommandName() {
		return "groovy";
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
			String fileName = commandString.substring(getCommandName().length()).trim();
			GroovyQueryScript script = scriptRegistry.newScript(fileName);
			return new GroovyQueryCommand(script);
		} catch (Throwable t) {
			slog.error("araqne logdb groovy: cannot instanciate groovy script", t);
			throw new QueryParseException("groovy-script-failure", -1, t.getMessage());
		}
	}
}
