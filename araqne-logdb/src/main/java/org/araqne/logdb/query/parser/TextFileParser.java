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
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.araqne.log.api.LogParser;
import org.araqne.log.api.LogParserFactory;
import org.araqne.log.api.LogParserFactoryRegistry;
import org.araqne.logdb.LogQueryCommand;
import org.araqne.logdb.LogQueryCommandParser;
import org.araqne.logdb.LogQueryContext;
import org.araqne.logdb.query.command.TextFile;

public class TextFileParser implements LogQueryCommandParser {

	private LogParserFactoryRegistry parserFactoryRegistry;

	public TextFileParser(LogParserFactoryRegistry parserFactoryRegistry) {
		this.parserFactoryRegistry = parserFactoryRegistry;
	}

	@Override
	public String getCommandName() {
		return "textfile";
	}

	@Override
	public LogQueryCommand parse(LogQueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(commandString, getCommandName().length(), new ArrayList<String>());
		Map<String, String> options = (Map<String, String>) r.value;
		String filePath = commandString.substring(r.next).trim();

		try {
			int offset = 0;
			if (options.containsKey("offset"))
				offset = Integer.valueOf(options.get("offset"));

			int limit = 0;
			if (options.containsKey("limit"))
				limit = Integer.valueOf(options.get("limit"));

			FileInputStream is = new FileInputStream(new File(filePath));
			String parserName = options.get("parser");
			LogParser parser = null;
			if (parserName != null) {
				LogParserFactory factory = parserFactoryRegistry.get(parserName);
				if (factory == null)
					throw new IllegalStateException("log parser not found: " + parserName);

				parser = factory.createParser(convert(options));
			}

			return new TextFile(is, parser, offset, limit);
		} catch (Throwable t) {
			throw new RuntimeException("cannot create textfile source", t);
		}
	}

	private Properties convert(Map<String, String> options) {
		Properties p = new Properties();
		for (String key : options.keySet()) {
			String value = options.get(key);
			if (value != null)
				p.put(key, value);
		}

		return p;
	}

}
