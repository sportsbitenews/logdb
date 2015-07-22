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
import java.util.HashMap;
import java.util.Map;

import org.araqne.log.api.LogParser;
import org.araqne.log.api.LogParserFactory;
import org.araqne.log.api.LogParserFactoryRegistry;
import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.TextFile;

public class TextFileParser extends AbstractQueryCommandParser {

	private LogParserFactoryRegistry parserFactoryRegistry;

	public TextFileParser(LogParserFactoryRegistry parserFactoryRegistry) {
		this.parserFactoryRegistry = parserFactoryRegistry;
	}

	@Override
	public String getCommandName() {
		return "textfile";
	}
	
	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("10700", new QueryErrorMessage("invalid-textfile-path", "[file]이 존재하지 않거나 읽을수 없습니다."));
		return m;
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(), new ArrayList<String>(),
				getFunctionRegistry());
		@SuppressWarnings("unchecked")
		Map<String, String> options = (Map<String, String>) r.value;
		String filePath = commandString.substring(r.next).trim();

		try {
			long offset = 0;
			if (options.containsKey("offset"))
				offset = Long.valueOf(options.get("offset"));

			long limit = 0;
			if (options.containsKey("limit"))
				limit = Long.valueOf(options.get("limit"));
			
			String brex = null;
			if (options.containsKey("brex"))
				brex = options.get("brex");
			
			String erex = null;
			if (options.containsKey("erex"))
				erex = options.get("erex");
			
			String df = null;
			if (options.containsKey("df"))
				df = options.get("df");
			
			String dp = null;
			if (options.containsKey("dp"))
				dp = options.get("dp");
			
			String cs = "utf-8";
			if (options.containsKey("cs"))
				cs = options.get("cs");

			File f = new File(filePath);
			if (!f.exists() || !f.canRead()){
			//	throw new QueryParseException("invalid-textfile-path", -1);
				Map<String, String> params = new HashMap<String, String>();
				params.put("file",filePath);
				int offsetS = QueryTokenizer.findKeyword(commandString, filePath, getCommandName().length());
				throw new QueryParseException("10700",  offsetS, offsetS + filePath.length() -1, params );
			}

			String parserName = options.get("parser");
			LogParser parser = null;
			if (parserName != null) {
				LogParserFactory factory = parserFactoryRegistry.get(parserName);
				if (factory == null)
					throw new IllegalStateException("log parser not found: " + parserName);

				parser = factory.createParser(options);
			}

			return new TextFile(filePath, parser, offset, limit, brex, erex, df, dp, cs);
		}catch(QueryParseException t){
			throw t;
		}catch (Throwable t) {
			throw new RuntimeException("cannot create textfile source", t);
		}
	}
}
