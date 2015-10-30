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
		setDescriptions("Write input tuples to JSON formatted text file. Each JSON literal is separated by new line.",
				"지정된 파일시스템 경로에 주어진 필드 값들을 JSON 포맷의 텍스트로 기록합니다. 각 JSON 레코드는 개행으로 구분됩니다.");

		setOptions("overwrite", false, "Use `overwrite=t` to overwrite existing file.",
				"t로 설정 시 이미 파일이 있더라도 덮어씁니다. 미설정 시 파일이 존재하면 쿼리가 실패합니다.");
		setOptions("encoding", false, "Specify output encoding. Default is `utf-8`.", "파일 인코딩을 지정합니다. 미설정 시 기본값은 utf-8입니다.");
		setOptions("tmp", false, "Write output to temporary file, and rename it at last.",
				"임시 파일경로를 설정할 경우 파일을 해당 경로에 임시로 작성한 후 쿼리가 종료되면 입력된 파일경로로 이동시킵니다.");
		setOptions("partition", false,
				"Use `partition=t` to separate output files by partition key. You can use `logtime` or `now` macro for partition key.",
				"t로 설정 시 파일 경로에 시간 기반으로 입력된 매크로를 기준으로 디렉토리를 설정할 수 있습니다. 로그 시각을 기준으로 하는 logtime 매크로와 현재 시각을 기준으로 하는 now 매크로를 사용할 수 있으며, 파티션 옵션을 지정하고 경로에 매크로를 사용하지 않으면 쿼리가 실패합니다.");
	}

	@Override
	public String getCommandName() {
		return "outputjson";
	}

	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("30300", new QueryErrorMessage("missing-field", "잘못된 쿼리문 입니다."));
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
			// throw new QueryParseException("missing-field",
			// commandString.length());
			throw new QueryParseException("30300", commandString.trim().length() - 1, commandString.trim().length() - 1, null);

		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(),
				Arrays.asList("overwrite", "tmp", "partition", "encoding", "append", "flush"), getFunctionRegistry());
		Map<String, String> options = (Map<String, String>) r.value;
		boolean overwrite = CommandOptions.parseBoolean(options.get("overwrite"));
		boolean usePartition = CommandOptions.parseBoolean(options.get("partition"));
		boolean append = CommandOptions.parseBoolean(options.get("append"));

		if (append && overwrite)
			// throw new QueryParseException("choose-overwrite-or-append", -1);
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
			// throw new QueryParseException("missing-field", tokens.size());
			throw new QueryParseException("30302", getCommandName().length() + 1, commandString.length() - 1, null);

		String filePath = tokens.string(0);
		filePath = ExpressionParser.evalContextReference(context, filePath, getFunctionRegistry());

		List<PartitionPlaceholder> holders = PartitionPlaceholder.parse(filePath);
		if (!usePartition && holders.size() > 0)
			// throw new QueryParseException("use-partition-option", -1,
			// holders.size() + " partition holders");
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

		// File jsonFile = new File(filePath);
		// if (jsonFile.exists() && !overwrite)
		// throw new IllegalStateException("json file exists: " +
		// jsonFile.getAbsolutePath());
		//
		// if (!usePartition && jsonFile.getParentFile() != null)
		// jsonFile.getParentFile().mkdirs();
		// return new OutputJson(jsonFile, filePath, overwrite, fields,
		// encoding, usePartition, tmpPath, holders);
		return new OutputJson(filePath, overwrite, fields, encoding, usePartition, tmpPath, holders, append, flushInterval,
				tickService);
	}
}
