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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryCommandParser;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.query.command.OutputCsv;

public class OutputCsvParser implements QueryCommandParser {

	@Override
	public String getCommandName() {
		return "outputcsv";
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		if (commandString.trim().endsWith(","))
			throw new QueryParseException("missing-field", commandString.length());

		boolean overwrite = false;
		boolean useBom = false;
		String encoding = null;
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(),
				Arrays.asList("overwrite", "encoding", "bom", "tab"));

		Map<String, String> options = (Map<String, String>) r.value;
		if (options != null && options.containsKey("overwrite"))
			overwrite = Boolean.parseBoolean(options.get("overwrite"));

		if (options != null && options.containsKey("bom"))
			useBom = Boolean.parseBoolean(options.get("bom"));

		if (options.get("encoding") != null)
			encoding = options.get("encoding").toString();
		if (encoding == null)
			encoding = "utf-8";

		boolean useTab = CommandOptions.parseBoolean(options.get("tab"));

		QueryTokens tokens = QueryTokenizer.tokenize(commandString.substring(r.next));
		List<String> fields = new ArrayList<String>();
		String originalCsvPath = tokens.string(0);
		String csvPath = ExpressionParser.evalContextReference(context, originalCsvPath);

		List<QueryToken> fieldTokens = tokens.subtokens(1, tokens.size());
		for (QueryToken t : fieldTokens) {
			StringTokenizer tok = new StringTokenizer(t.token, ",");
			while (tok.hasMoreTokens())
				fields.add(tok.nextToken().trim());
		}

		if (fields.size() == 0)
			throw new QueryParseException("missing-field", commandString.length());

		File csvFile = new File(csvPath);
		if (csvFile.exists() && !overwrite)
			throw new IllegalStateException("csv file exists: " + csvFile.getAbsolutePath());

		try {
			if (csvFile.getParentFile() != null)
				csvFile.getParentFile().mkdirs();
			return new OutputCsv(originalCsvPath, csvFile, overwrite, fields, encoding, useBom, useTab);
		} catch (IOException e) {
			throw new QueryParseException("io-error", -1, e.getMessage());
		}
	}
}
