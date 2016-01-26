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
import java.util.HashMap;
import java.util.Map;

import org.araqne.log.api.LogParser;
import org.araqne.log.api.LogParserFactory;
import org.araqne.log.api.LogParserFactoryRegistry;
import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.FilePathHelper;
import org.araqne.logdb.LocalFilePathHelper;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
import org.araqne.logdb.QueryParseException;

public class ZipFileParser extends AbstractQueryCommandParser {
	private LogParserFactoryRegistry parserFactoryRegistry;

	public ZipFileParser(LogParserFactoryRegistry parserFactoryRegistry) {
		this.parserFactoryRegistry = parserFactoryRegistry;

		setDescriptions(
				"Read zipped text file. You can recognize multi-line entry using regular expressions. Each tuple has 'line' field.",
				"ZIP으로 압축된 텍스트 파일에서 데이터를 조회합니다. 정규표현식으로 사용하여 여러개의 줄로 구성된 데이터의 시작과 끝을 인식시킬 수 있습니다. \" + \"텍스트 파일에서 조회한 각 레코드는 line 필드를 포함합니다.");

		setOptions("offset", OPTIONAL, "Skip output count", "건너뛸 레코드 갯수");
		setOptions("limit", OPTIONAL, "Max output count", "가져올 최대 레코드 갯수");
	}

	@Override
	public String getCommandName() {
		return "zipfile";
	}

	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("14000", new QueryErrorMessage("invalid-zipfile-path", "[file]이 존재하지 않거나 읽을수 없습니다."));
		m.put("14001", new QueryErrorMessage("invalid-parentfile-path", "[file]의 상위 디렉토리가 존재하지 않거나 읽을 수 없습니다."));
		m.put("10402", new QueryErrorMessage("missing-field", "파일경로 또는 엔트리 경로를 입력하십시오."));
		return m;
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(), new ArrayList<String>(),
				getFunctionRegistry());
		@SuppressWarnings("unchecked")
		Map<String, String> options = (Map<String, String>) r.value;
		QueryTokens tokens = QueryTokenizer.tokenize(commandString.substring(r.next).trim());

		if (tokens.size() < 2)
			throw new QueryParseException("10402", commandString.trim().length() - 1, commandString.trim().length() - 1, null);

		String filePath = tokens.reverseArg(1);
		String entryPath = tokens.lastArg();

		if (filePath.trim().isEmpty() || entryPath.trim().isEmpty())
			throw new QueryParseException("10402", commandString.trim().length() - 1, commandString.trim().length() - 1, null);

		try {

			int offset = 0;
			if (options.containsKey("offset"))
				offset = Integer.valueOf(options.get("offset"));

			int limit = 0;
			if (options.containsKey("limit"))
				limit = Integer.valueOf(options.get("limit"));

			String parserName = options.get("parser");
			LogParser parser = null;
			if (parserName != null) {
				LogParserFactory factory = parserFactoryRegistry.get(parserName);
				if (factory == null)
					throw new IllegalStateException("log parser not found: " + parserName);

				parser = factory.createParser(options);
			}

			FilePathHelper pathHelper = new LocalFilePathHelper(filePath);

			return new org.araqne.logdb.query.command.ZipFile(pathHelper.getMatchedFilePaths(), filePath, entryPath, parser,
					offset, limit);
		} catch (IllegalStateException e) {
			String msg = e.getMessage();
			Map<String, String> params = new HashMap<String, String>();
			params.put("file", filePath);
			int offsetS = QueryTokenizer.findKeyword(commandString, filePath, getCommandName().length());
			String type = null;
			if (msg.equals("file-not-found"))
				type = "14000";
			else
				type = "14001";

			throw new QueryParseException(type, offsetS, offsetS + filePath.length() - 1, params);
		} catch (Throwable t) {
			throw new RuntimeException("cannot create zipfile source", t);
		}
	}
}
