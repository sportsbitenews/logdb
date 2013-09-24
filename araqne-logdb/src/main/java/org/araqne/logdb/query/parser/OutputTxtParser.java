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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.araqne.logdb.LogQueryCommand;
import org.araqne.logdb.LogQueryCommandParser;
import org.araqne.logdb.LogQueryContext;
import org.araqne.logdb.LogQueryParseException;
import org.araqne.logdb.query.command.OutputTxt;

/**
 * @since 1.6.7
 * @author darkluster
 * 
 */
public class OutputTxtParser implements LogQueryCommandParser {

	@Override
	public String getCommandName() {
		return "outputtxt";
	}

	@Override
	public LogQueryCommand parse(LogQueryContext context, String commandString) {
		if (commandString.trim().endsWith(","))
			throw new LogQueryParseException("missing-field", commandString.length());

		String delimiter = null;
		ParseResult r = QueryTokenizer.parseOptions(commandString, "outputtxt".length(), Arrays.asList("delimiter"));

		@SuppressWarnings("unchecked")
		Map<String, Object> options = (Map<String, Object>) r.value;
		if (options.containsKey("delimiter"))
			delimiter = options.get("delimiter").toString();
		if (delimiter == null)
			delimiter = " ";

		int next = r.next;
		if (next < 0)
			throw new LogQueryParseException("invalid-field", next);
		String remainCommandString = commandString.substring(next);
		QueryTokens tokens = QueryTokenizer.tokenize(remainCommandString);
		if (tokens.size() < 1)
			throw new LogQueryParseException("missing-field", tokens.size());

		String filePath = tokens.token(0).token;
		List<String> fields = new ArrayList<String>();

		List<QueryToken> queryFields = tokens.subtokens(1, tokens.size());
		for (QueryToken token : queryFields) {
			if (!token.token.contains(",")) {
				fields.add(token.token.trim());
				continue;
			}

			StringTokenizer tok = new StringTokenizer(token.token, ",");
			while (tok.hasMoreTokens())
				fields.add(tok.nextToken().trim());
		}

		if (fields.size() == 0)
			throw new LogQueryParseException("missing-field", remainCommandString.length());

		File txtFile = new File(filePath);
		if (txtFile.exists())
			throw new IllegalStateException("txt file exists: " + txtFile.getAbsolutePath());

		try {
			if (txtFile.getParentFile() != null)
				txtFile.getParentFile().mkdirs();
			return new OutputTxt(txtFile, delimiter, fields);
		} catch (IOException e) {
			throw new LogQueryParseException("io-error", -1, e.getMessage());
		}
	}
}
