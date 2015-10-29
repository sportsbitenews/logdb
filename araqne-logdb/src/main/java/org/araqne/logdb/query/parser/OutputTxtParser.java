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
		setDescriptions("Write input tuples to text file.", "지정된 파일시스템 경로에 주어진 필드 값들을 텍스트 파일로 기록합니다.");

		setOptions("overwrite", false, "Use `overwrite=t` to overwrite existing file.",
				"t로 설정 시 이미 파일이 있더라도 덮어씁니다. 미설정 시 파일이 존재하면 쿼리가 실패합니다.");
		setOptions("delimiter", false, "Delimiter character. Default is white-space.", "미지정 시 기본값은 공백 문자입니다.");
		setOptions("gz", false, "Use `gz=t` to compress file.", "t로 설정 시 결과 텍스트 파일이 gz 형식으로 압축됩니다.");
		setOptions("encoding", false, "Specify output encoding. Default is `utf-8`.", "파일 인코딩을 지정합니다. 미설정 시 기본값은 utf-8입니다.");
		setOptions("tmp", false, "Write output to temporary file, and rename it at last.",
				"임시 파일경로를 설정할 경우 파일을 해당 경로에 임시로 작성한 후 쿼리가 종료되면 입력된 파일경로로 이동시킵니다.");
		setOptions("partition", false,
				"Use `partition=t` to separate output files by partition key. You can use `logtime` or `now` macro for partition key.",
				"t로 설정 시 파일 경로에 시간 기반으로 입력된 매크로를 기준으로 디렉토리를 설정할 수 있습니다. 로그 시각을 기준으로 하는 logtime 매크로와 현재 시각을 기준으로 하는 now 매크로를 사용할 수 있으며, 파티션 옵션을 지정하고 경로에 매크로를 사용하지 않으면 쿼리가 실패합니다.");
	}

	@Override
	public String getCommandName() {
		return "outputtxt";
	}

	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("30400", new QueryErrorMessage("missing-field", "잘못된 쿼리문 입니다."));
		m.put("30401", new QueryErrorMessage("use-partition-option", "파티션(partition) 옵션이 필요합니다."));
		m.put("30402", new QueryErrorMessage("missing-field", "필드명을 입력하십시오."));
		m.put("30403", new QueryErrorMessage("temp-file-exists", "[temp](temp) 파일이 존재합니다."));
		m.put("30404", new QueryErrorMessage("missing-field", "출력파일명 및 필드 값을 입력하시오."));
		m.put("30405", new QueryErrorMessage("missing-field", "출력파일명 및 필드 값을 입력하시오."));
		m.put("30406", new QueryErrorMessage("io-error", " IO 에러가 발생했습니다: [msg]."));
		m.put("30407", new QueryErrorMessage("choose-overwrite-or-append", "overwrite 와 append 옵션은 동시에 사용할 수 없습니다."));

		return m;
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		if (commandString.trim().endsWith(","))
			// throw new QueryParseException("missing-field",
			// commandString.length());
			throw new QueryParseException("30400", commandString.trim().length() - 1, commandString.trim().length() - 1, null);

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
			// throw new QueryParseException("choose-overwrite-or-append", -1);
			throw new QueryParseException("30407", -1, -1, null);

		File tmpFile = null;
		if (tmpPath != null) {
			tmpFile = new File(tmpPath);
			if (!usePartition && tmpFile.exists()) {
				// throw new QueryParseException("tmp file exist: " + tmpPath,
				// -1);
				Map<String, String> params = new HashMap<String, String>();
				params.put("temp", tmpPath);
				int offset = QueryTokenizer.indexOfValue(commandString, "tmp=" + tmpPath) + 1;
				throw new QueryParseException("30403", offset, offset + tmpPath.length() - 1, params);
			}
		}

		int next = r.next;
		if (next < 0)
			// throw new QueryParseException("invalid-field", next);
			throw new QueryParseException("30404", getCommandName().length() + 1, commandString.length() - 1, null);

		String remainCommandString = commandString.substring(next);
		QueryTokens tokens = QueryTokenizer.tokenize(remainCommandString);
		if (tokens.size() < 1)
			// throw new QueryParseException("missing-field", tokens.size());
			throw new QueryParseException("30405", getCommandName().length() + 1, commandString.length() - 1, null);

		String filePath = tokens.token(0).token;
		filePath = ExpressionParser.evalContextReference(context, filePath, getFunctionRegistry());

		List<PartitionPlaceholder> holders = PartitionPlaceholder.parse(filePath);
		if (!usePartition && holders.size() > 0)
			// throw new QueryParseException("use-partition-option", -1,
			// holders.size() + " partition holders");
			throw new QueryParseException("30401", getCommandName().length() + 1, commandString.length() - 1, null);

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

			// throw new QueryParseException("missing-field",
			// remainCommandString.length());
			throw new QueryParseException("30402", getCommandName().length() + 1, commandString.length() - 1, null);

		// File txtFile = new File(filePath);
		// if (txtFile.exists() && !overwrite)
		// throw new IllegalStateException("txt file exists: " +
		// txtFile.getAbsolutePath());
		//
		// if (!usePartition && txtFile.getParentFile() != null)
		// txtFile.getParentFile().mkdirs();
		//
		// return new OutputTxt(txtFile, filePath, tmpPath, overwrite,
		// delimiter, fields, useGzip, encoding, usePartition, holders);
		return new OutputTxt(filePath, tmpPath, overwrite, delimiter, fields, true, useGzip, encoding, usePartition, holders,
				append, flushInterval, tickService);
	}
}
