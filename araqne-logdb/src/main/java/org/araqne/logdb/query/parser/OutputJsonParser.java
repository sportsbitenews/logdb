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
package org.araqne.logdb.query.parser;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.araqne.logdb.LogQueryCommand;
import org.araqne.logdb.LogQueryCommandParser;
import org.araqne.logdb.LogQueryContext;
import org.araqne.logdb.LogQueryParseException;
import org.araqne.logdb.query.command.OutputJson;

/**
 * @since 1.6.7
 * @author darkluster
 * 
 */
public class OutputJsonParser implements LogQueryCommandParser {

	@Override
	public String getCommandName() {
		return "outputjson";
	}

	@Override
	public LogQueryCommand parse(LogQueryContext context, String commandString) {
		if (commandString.trim().endsWith(","))
			throw new LogQueryParseException("missing-field", commandString.length());

		QueryTokens tokens = QueryTokenizer.tokenize(commandString);
		if (tokens.size() < 2)
			throw new LogQueryParseException("missing-field", tokens.size());

		String filePath = tokens.firstArg();
		List<String> fields = new ArrayList<String>();

		List<QueryToken> queryFields = tokens.subtokens(2, tokens.size());
		for (QueryToken token : queryFields) {
			if (!token.token.contains(",")) {
				fields.add(token.token.trim());
				continue;
			}

			StringTokenizer tok = new StringTokenizer(token.token, ",");
			while (tok.hasMoreTokens())
				fields.add(tok.nextToken().trim());
		}

		File jsonFile = new File(filePath);
		if (jsonFile.exists())
			throw new IllegalStateException("json file exists: " + jsonFile.getAbsolutePath());

		if (jsonFile.getParentFile() != null)
			jsonFile.getParentFile().mkdirs();
		return new OutputJson(jsonFile, fields);
	}

}
