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

import org.araqne.cron.TickService;
import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.PartitionPlaceholder;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.TimeSpan;
import org.araqne.logdb.query.command.OutputTxt;

/**
 * @since 1.6.7
 * @author darkluster
 * 
 */
public class OutputTxtParser extends AbstractQueryCommandParser {

	private TickService tickService;

	public OutputTxtParser(TickService tickService) {
		this.tickService = tickService;
	}

	@Override
	public String getCommandName() {
		return "outputtxt";
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		if (commandString.trim().endsWith(","))
			throw new QueryParseException("missing-field", commandString.length());

		ParseResult r = QueryTokenizer.parseOptions(context, commandString, "outputtxt".length(),
				Arrays.asList("delimiter", "overwrite", "gz", "encoding", "partition", "tmp", "append", "flush"),
				getFunctionRegistry());

		@SuppressWarnings("unchecked")
		Map<String, String> options = (Map<String, String>) r.value;

		String delimiter = options.get("delimiter");
		if (delimiter == null)
			delimiter = " ";

		String encoding = options.get("encoding");
		if (encoding == null)
			encoding = "utf-8";

		TimeSpan flushInterval = null;
		if (options.containsKey("flush"))
			flushInterval = TimeSpan.parse(options.get("flush"));

		String tmpPath = options.get("tmp");

		boolean overwrite = CommandOptions.parseBoolean(options.get("overwrite"));
		boolean useGzip = CommandOptions.parseBoolean(options.get("gz"));
		boolean usePartition = CommandOptions.parseBoolean(options.get("partition"));
		boolean append = CommandOptions.parseBoolean(options.get("append"));

		if (append && overwrite)
			throw new QueryParseException("choose-overwrite-or-append", -1);

		File tmpFile = null;
		if (tmpPath != null) {
			tmpFile = new File(tmpPath);
			if (!usePartition && tmpFile.exists())
				throw new QueryParseException("tmp file exist: " + tmpPath, -1);
		}

		int next = r.next;
		if (next < 0)
			throw new QueryParseException("invalid-field", next);

		String remainCommandString = commandString.substring(next);
		QueryTokens tokens = QueryTokenizer.tokenize(remainCommandString);
		if (tokens.size() < 1)
			throw new QueryParseException("missing-field", tokens.size());

		String filePath = tokens.token(0).token;
		filePath = ExpressionParser.evalContextReference(context, filePath, getFunctionRegistry());

		List<PartitionPlaceholder> holders = PartitionPlaceholder.parse(filePath);
		if (!usePartition && holders.size() > 0)
			throw new QueryParseException("use-partition-option", -1, holders.size() + " partition holders");

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
			throw new QueryParseException("missing-field", remainCommandString.length());

		File txtFile = new File(filePath);
		if (txtFile.exists() && !overwrite && !append)
			throw new IllegalStateException("txt file exists: " + txtFile.getAbsolutePath());

		if (!usePartition && txtFile.getParentFile() != null)
			txtFile.getParentFile().mkdirs();
		
		// TODO I'll fix 7th parameter
		return new OutputTxt(txtFile, filePath, tmpPath, overwrite, delimiter, fields, true, useGzip, encoding, usePartition, holders,
				append, flushInterval, tickService);
	}
}
