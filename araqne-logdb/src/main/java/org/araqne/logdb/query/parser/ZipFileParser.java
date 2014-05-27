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
import java.util.Map;

import org.araqne.log.api.LogParser;
import org.araqne.log.api.LogParserFactory;
import org.araqne.log.api.LogParserFactoryRegistry;
import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;

public class ZipFileParser extends AbstractQueryCommandParser {
	private LogParserFactoryRegistry parserFactoryRegistry;

	public ZipFileParser(LogParserFactoryRegistry parserFactoryRegistry) {
		this.parserFactoryRegistry = parserFactoryRegistry;
	}

	@Override
	public String getCommandName() {
		return "zipfile";
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		try {
			QueryTokens tokens = QueryTokenizer.tokenize(commandString);
			Map<String, String> options = tokens.options();

			String filePath = tokens.reverseArg(1);
			String entryPath = tokens.lastArg();

			int offset = 0;
			if (options.containsKey("offset"))
				offset = Integer.valueOf(options.get("offset"));

			int limit = 0;
			if (options.containsKey("limit"))
				limit = Integer.valueOf(options.get("limit"));

			File file = new File(filePath);
			if (!file.exists())
				throw new IllegalStateException("zipfile [" + file.getAbsolutePath() + "] not found");

			if (!file.canRead())
				throw new IllegalStateException("cannot read zipfile [" + file.getAbsolutePath() + "], check read permission");

			String parserName = options.get("parser");
			LogParser parser = null;
			if (parserName != null) {
				LogParserFactory factory = parserFactoryRegistry.get(parserName);
				if (factory == null)
					throw new IllegalStateException("log parser not found: " + parserName);

				parser = factory.createParser(options);
			}

			return new org.araqne.logdb.query.command.ZipFile(filePath, entryPath, parser, offset, limit);
		} catch (Throwable t) {
			throw new RuntimeException("cannot create zipfile source", t);
		}
	}
}
