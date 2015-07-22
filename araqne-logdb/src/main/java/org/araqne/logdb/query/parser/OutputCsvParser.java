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
import org.araqne.logdb.query.command.OutputCsv;

public class OutputCsvParser extends AbstractQueryCommandParser {
	private TickService tickService;
	private boolean defaultUseBom;

	public OutputCsvParser(TickService tickService) {
		this(tickService, false);
	}

	public OutputCsvParser(TickService tickService, boolean useBom) {
		this.tickService = tickService;
		this.defaultUseBom = useBom;
	}

	@Override
	public String getCommandName() {
		return "outputcsv";
	}

	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("30200", new QueryErrorMessage("missing-field","잘못된 쿼리문 입니다."));
		m.put("30201", new QueryErrorMessage("use-partition-option", "파티션(partition) 옵션이 필요합니다."));
		m.put("30202", new QueryErrorMessage("missing-field", "필드명을 입력하십시오.")); 
		m.put("30203", new QueryErrorMessage("io-error", "IO 에러가 발생했습니다: [msg].")); 
		m.put("30204", new QueryErrorMessage("unsuported-encoding", "[encoding]은 지원하지 않는 인코딩입니다.")); 
		m.put("30205", new QueryErrorMessage("choose-overwrite-or-append", "overwrite 와 append 옵션은 동시에 사용할 수 없습니다.")); 
		return m;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		if (commandString.trim().endsWith(","))
		//	throw new QueryParseException("missing-field", commandString.length());
			throw new QueryParseException("30200", commandString.trim().length() -1, commandString.trim().length() -1 , null);

		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(),
				Arrays.asList("overwrite", "encoding", "bom", "tab", "tmp", "partition", "emptyfile", "append", "flush"),
				getFunctionRegistry());

		Map<String, String> options = (Map<String, String>) r.value;
		boolean overwrite = CommandOptions.parseBoolean(options.get("overwrite"));

		boolean useBom = false;
		if (options.get("bom") == null)
			useBom = this.defaultUseBom;
		else
			useBom = CommandOptions.parseBoolean(options.get("bom"));

		boolean useTab = CommandOptions.parseBoolean(options.get("tab"));
		boolean usePartition = CommandOptions.parseBoolean(options.get("partition"));
		boolean emptyfile = CommandOptions.parseBoolean(options.get("emptyfile"));
		boolean append = CommandOptions.parseBoolean(options.get("append"));
		
		if (append && overwrite)
		//	throw new QueryParseException("choose-overwrite-or-append", -1);
			throw new QueryParseException("30305", -1, -1, null);
		

		String encoding = options.get("encoding");
		if (encoding == null)
			encoding = "utf-8";

		TimeSpan flushInterval = null;
		if (options.containsKey("flush"))
			flushInterval = TimeSpan.parse(options.get("flush"));

		String tmpPath = options.get("tmp");

		QueryTokens tokens = QueryTokenizer.tokenize(commandString.substring(r.next));
		List<String> fields = new ArrayList<String>();
		String originalCsvPath = tokens.string(0);
		String csvPath = ExpressionParser.evalContextReference(context, originalCsvPath, getFunctionRegistry());

		List<PartitionPlaceholder> holders = PartitionPlaceholder.parse(csvPath);
		if (!usePartition && holders.size() > 0)
		//	throw new QueryParseException("use-partition-option", -1, holders.size() + " partition holders");
			throw new QueryParseException("30201", getCommandName().length() + 1, commandString.length() - 1, null);
			
		List<QueryToken> fieldTokens = tokens.subtokens(1, tokens.size());
		for (QueryToken t : fieldTokens) {
			StringTokenizer tok = new StringTokenizer(t.token, ",");
			while (tok.hasMoreTokens())
				fields.add(tok.nextToken().trim());
		}

		if (fields.size() == 0)
	//		throw new QueryParseException("missing-field", commandString.length());
			throw new QueryParseException("30202", getCommandName().length() + 1 , commandString.length() - 1, null) ;

		return new OutputCsv(csvPath,  tmpPath, overwrite, fields, encoding, useBom, useTab, usePartition,
				emptyfile, holders, append, flushInterval, tickService);
	}
}
