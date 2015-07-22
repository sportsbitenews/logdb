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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.araqne.cron.TickService;
import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.PartitionPlaceholder;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.TimeSpan;
import org.araqne.logdb.query.command.OutputJson;

/**
 * @since 1.6.7
 * @author darkluster
 * 
 */
public class OutputJsonParser extends AbstractQueryCommandParser {
	private TickService tickService;

	public OutputJsonParser(TickService tickService) {
		this.tickService = tickService;
	}

	@Override
	public String getCommandName() {
		return "outputjson";
	}
	
	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("30300", new QueryErrorMessage("missing-field","잘못된 쿼리문 입니다."));
		m.put("30301", new QueryErrorMessage("use-partition-option", "파티션(partition) 옵션이 필요합니다."));
		m.put("30302", new QueryErrorMessage("missing-field", "필드명을 입력하십시오.")); 
		m.put("30303", new QueryErrorMessage("io-error", "IO 에러가 발생했습니다: [msg].")); 
		m.put("30304", new QueryErrorMessage("choose-overwrite-or-append", "overwrite 와 append 옵션은 동시에 사용할 수 없습니다.")); 
		
		return m;
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		if (commandString.trim().endsWith(","))
		//	throw new QueryParseException("missing-field", commandString.length());
			throw new QueryParseException("30300", commandString.trim().length() -1, commandString.trim().length() -1 , null);
			
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(),
				Arrays.asList("overwrite", "tmp", "partition", "encoding", "append", "flush"), getFunctionRegistry());
		Map<String, String> options = (Map<String, String>) r.value;
		boolean overwrite = CommandOptions.parseBoolean(options.get("overwrite"));
		boolean usePartition = CommandOptions.parseBoolean(options.get("partition"));
		boolean append = CommandOptions.parseBoolean(options.get("append"));

		if (append && overwrite)
		//	throw new QueryParseException("choose-overwrite-or-append", -1);
			 throw new QueryParseException("30304", -1, -1, null);

		String encoding = options.get("encoding");
		if (encoding == null)
			encoding = "utf-8";

		TimeSpan flushInterval = null;
		if (options.containsKey("flush"))
			flushInterval = TimeSpan.parse(options.get("flush"));

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
		return new OutputJson(filePath, overwrite, fields, encoding, usePartition, tmpPath, holders, append,
				flushInterval, tickService);
	}
}
