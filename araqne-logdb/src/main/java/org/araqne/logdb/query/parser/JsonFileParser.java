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
import org.araqne.logdb.query.command.JsonFile;

public class JsonFileParser extends AbstractQueryCommandParser {

	private LogParserFactoryRegistry parserFactoryRegistry;

	public JsonFileParser(LogParserFactoryRegistry parserFactoryRegistry) {
		this.parserFactoryRegistry = parserFactoryRegistry;
		setDescriptions("Read JSON literal from text file line by line.",
				"CR LF 혹은 LF로 구분되는 줄 단위로 JSON을 포함하는 텍스트 파일에서 데이터를 조회합니다. JSON의 키/값은 자동으로 로그프레소 타입으로 매핑되어 파싱됩니다.");

		setOptions("offset", false, "Skip count", "건너뛸 레코드 갯수");
		setOptions("limit", false, "Max output count", "가져올 최대 레코드 갯수");
		setOptions("parser", false, "Parse tuple using parser.", "JSON 파싱 후, 별도의 파서를 추가로 적용하려면 파서 이름을 입력합니다.");
		setOptions("overlay", false, "Use `overlay=t` option if you want to override parsed fields on original data.",
				"파서를 추가로 적용하는 경우, 원본 데이터를 그대로 유지하면서 파싱된 키/값을 덮어쓰려면 overlay 옵션의 값을 t로 지정합니다. 만약 overlay를 t로 지정하지 않고 parser를 적용하면, 원본 대신 파싱된 키/값만 출력됩니다.");
	}

	@Override
	public String getCommandName() {
		return "jsonfile";
	}

	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("10900", new QueryErrorMessage("invalid-jsonfile-path", "[file]이 존재하지 않거나 읽을수 없습니다"));
		m.put("10901", new QueryErrorMessage("invalid-parentfile-path", "[file]의 상위 디렉토리가 존재하지 않거나 읽을 수 없습니다."));
		return m;
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(), new ArrayList<String>(),
				getFunctionRegistry());
		@SuppressWarnings("unchecked")
		Map<String, String> options = (Map<String, String>) r.value;
		String filePath = commandString.substring(r.next).trim();
		filePath = ExpressionParser.evalContextReference(context, filePath, getFunctionRegistry());

		try {
			boolean overlay = false;
			long offset = 0;
			if (options.containsKey("offset"))
				offset = Integer.valueOf(options.get("offset"));

			long limit = 0;
			if (options.containsKey("limit"))
				limit = Integer.valueOf(options.get("limit"));

			if (options.containsKey("overlay")) {
				String o = options.get("overlay");
				overlay = o.equals("t") || o.equals("1") || o.equals("true");
			}

			FilePathHelper pathHelper = new LocalFilePathHelper(filePath);

			String parserName = options.get("parser");
			LogParser parser = null;
			if (parserName != null) {
				LogParserFactory factory = parserFactoryRegistry.get(parserName);
				if (factory == null)
					throw new IllegalStateException("log parser not found: " + parserName);

				parser = factory.createParser(options);
			}

			String parseTarget = options.get("parsetarget");

			return new JsonFile(pathHelper.getMatchedFilePaths(), filePath, parser, parseTarget, overlay, offset, limit);
		} catch (IllegalStateException e) {
			String msg = e.getMessage();
			Map<String, String> params = new HashMap<String, String>();
			params.put("file", filePath);
			int offsetS = QueryTokenizer.findKeyword(commandString, filePath, getCommandName().length());
			String type = null;
			if (msg.equals("file-not-found"))
				type = "10900";
			else
				type = "10901";

			throw new QueryParseException(type, offsetS, offsetS + filePath.length() - 1, params);
		} catch (Throwable t) {
			throw new RuntimeException("cannot create jsonfile source", t);
		}
	}
}
