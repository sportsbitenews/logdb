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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryCommandParser;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.OutputJson;

/**
 * @since 1.6.7
 * @author darkluster
 * 
 */
public class OutputJsonParser implements QueryCommandParser {

	@Override
	public String getCommandName() {
		return "outputjson";
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		if (commandString.trim().endsWith(","))
			throw new QueryParseException("missing-field", commandString.length());

		boolean overwrite = false;
		ParseResult r = QueryTokenizer
				.parseOptions(context, commandString, getCommandName().length(), Arrays.asList("overwrite"));
		Map<String, String> options = (Map<String, String>) r.value;
		if (options != null && options.containsKey("overwrite"))
			overwrite = Boolean.parseBoolean(options.get("overwrite"));

		QueryTokens tokens = QueryTokenizer.tokenize(commandString.substring(r.next));
		if (tokens.size() < 1)
			throw new QueryParseException("missing-field", tokens.size());

		String filePath = tokens.string(0);
		filePath = ExpressionParser.evalContextReference(context, filePath);

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

		File jsonFile = new File(filePath);
		if (jsonFile.exists() && !overwrite)
			throw new IllegalStateException("json file exists: " + jsonFile.getAbsolutePath());

		if (jsonFile.getParentFile() != null)
			jsonFile.getParentFile().mkdirs();
		return new OutputJson(jsonFile, filePath, overwrite, fields);
	}
}
