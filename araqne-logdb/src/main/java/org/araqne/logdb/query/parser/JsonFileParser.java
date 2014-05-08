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

import java.io.File;
import java.util.ArrayList;
import java.util.Map;

import org.araqne.log.api.LogParser;
import org.araqne.log.api.LogParserFactory;
import org.araqne.log.api.LogParserFactoryRegistry;
import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.JsonFile;

public class JsonFileParser extends AbstractQueryCommandParser {

	private LogParserFactoryRegistry parserFactoryRegistry;

	public JsonFileParser(LogParserFactoryRegistry parserFactoryRegistry) {
		this.parserFactoryRegistry = parserFactoryRegistry;
	}

	@Override
	public String getCommandName() {
		return "jsonfile";
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(), new ArrayList<String>(),
				getFunctionRegistry());
		@SuppressWarnings("unchecked")
		Map<String, String> options = (Map<String, String>) r.value;
		String filePath = commandString.substring(r.next).trim();

		try {
			boolean overlay = false;
			int offset = 0;
			if (options.containsKey("offset"))
				offset = Integer.valueOf(options.get("offset"));

			int limit = 0;
			if (options.containsKey("limit"))
				limit = Integer.valueOf(options.get("limit"));

			if (options.containsKey("overlay")) {
				String o = options.get("overlay");
				overlay = o.equals("t") || o.equals("1") || o.equals("true");
			}

			File f = new File(filePath);
			if (!f.exists() || !f.canRead())
				throw new QueryParseException("invalid-jsonfile-path", -1);

			String parserName = options.get("parser");
			LogParser parser = null;
			if (parserName != null) {
				LogParserFactory factory = parserFactoryRegistry.get(parserName);
				if (factory == null)
					throw new IllegalStateException("log parser not found: " + parserName);

				parser = factory.createParser(options);
			}

			String parseTarget = options.get("parsetarget");

			return new JsonFile(filePath, parser, parseTarget, overlay, offset, limit);
		} catch (Throwable t) {
			throw new RuntimeException("cannot create jsonfile source", t);
		}
	}
}
