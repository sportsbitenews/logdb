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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.PartitionPlaceholder;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.OutputJson;

/**
 * @since 1.6.7
 * @author darkluster
 * 
 */
public class OutputJsonParser extends AbstractQueryCommandParser {

	@Override
	public String getCommandName() {
		return "outputjson";
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		if (commandString.trim().endsWith(","))
		//	throw new QueryParseException("missing-field", commandString.length());
			throw new QueryParseException("30300", commandString.trim().length() -1, commandString.trim().length() -1 , null);
			
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(),
				Arrays.asList("overwrite", "tmp", "partition", "encoding"), getFunctionRegistry());
		Map<String, String> options = (Map<String, String>) r.value;
		boolean overwrite = CommandOptions.parseBoolean(options.get("overwrite"));
		boolean usePartition = CommandOptions.parseBoolean(options.get("partition"));

		String encoding = options.get("encoding");
		if (encoding == null)
			encoding = "utf-8";
		String tmpPath = options.get("tmp");

		QueryTokens tokens = QueryTokenizer.tokenize(commandString.substring(r.next));
		if (tokens.size() < 1)
		//	throw new QueryParseException("missing-field", tokens.size());
			throw new QueryParseException("30302", getCommandName().length() + 1 , commandString.length() - 1, null) ;
		
		String filePath = tokens.string(0);
		filePath = ExpressionParser.evalContextReference(context, filePath, getFunctionRegistry());

		List<PartitionPlaceholder> holders = PartitionPlaceholder.parse(filePath);
		if (!usePartition && holders.size() > 0)
		//	throw new QueryParseException("use-partition-option", -1, holders.size() + " partition holders");
			throw new QueryParseException("30301", getCommandName().length() + 1, commandString.length() - 1, null);


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

//		File jsonFile = new File(filePath);
//		if (jsonFile.exists() && !overwrite)
//			throw new IllegalStateException("json file exists: " + jsonFile.getAbsolutePath());
//
//		if (!usePartition && jsonFile.getParentFile() != null)
//			jsonFile.getParentFile().mkdirs();
//	  	return new OutputJson(jsonFile, filePath, overwrite, fields, encoding, usePartition, tmpPath, holders);
		return new OutputJson(filePath, overwrite, fields, encoding, usePartition, tmpPath, holders);
	}
}
