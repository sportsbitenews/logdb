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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.araqne.log.api.LogParser;
import org.araqne.log.api.LogParserFactory;
import org.araqne.log.api.LogParserFactoryRegistry;
import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.TextFile;

public class TextFileParser extends AbstractQueryCommandParser {

	private LogParserFactoryRegistry parserFactoryRegistry;

	public TextFileParser(LogParserFactoryRegistry parserFactoryRegistry) {
		this.parserFactoryRegistry = parserFactoryRegistry;

		setDescriptions(
				"Read text file. You can recognize multi-line entry using regular expressions. Each tuple has 'line' field.",
				"텍스트 파일에서 데이터를 조회합니다. 정규표현식으로 사용하여 여러개의 줄로 구성된 데이터의 시작과 끝을 인식시킬 수 있습니다. " + "텍스트 파일에서 조회한 각 레코드는 line 필드를 포함합니다.");

		setOptions("offset", OPTIONAL, "Skip output count", "건너뛸 레코드 갯수");
		setOptions("limit", OPTIONAL, "Max output count", "가져올 최대 레코드 갯수");
		setOptions("brex", OPTIONAL, "Regular expression for recognizing the first line of entry.",
				"다수의 줄로 구성된 하나의 레코드를 구분할 수 있도록, 레코드 시작 줄을 판정하는 정규표현식을 입력합니다. "
						+ "brex 정규표현식이 매칭되는 줄이 나오기 전까지 하나의 레코드로 병합합니다. 미지정 시 CR LF 혹은 LF 기준으로 각 줄을 하나의 레코드로 인식합니다.");
		setOptions("erex", OPTIONAL, "Regular expression for recognizing the last line of entry.",
				"다수의 줄로 구성된 하나의 레코드를 구분할 수 있도록, 레코드 마지막 줄을 판정하는 정규표현식을 입력합니다. "
						+ "erex 정규표현식이 매칭되는 줄이 나오기 전까지 하나의 레코드로 병합합니다. 미지정 시 CR LF 혹은 LF 기준으로 각 줄을 하나의 레코드로 인식합니다.");
		setOptions("df", OPTIONAL, "Date format. If not specified, _time will be current timestamp.",
				"dp 옵션으로 날짜 추출 정규표현식을 입력하면, df 옵션으로 지정된 타임스탬프 포맷으로 파싱하여 _time 필드를 추출합니다. 미지정 시 _time 필드 값이 데이터 로딩 시점의 시각으로 결정됩니다.");
		setOptions("dp", OPTIONAL,
				"Regular expression for extracting timestamp. For example, you can use 'yyyy-MM-dd HH:mm:ss.SSS'. "
						+ "If not specified, _time will be current timestamp.",
				"_time 필드 추출에 필요한 타임스탬프 포맷을 입력합니다. 예를 들어, yyyy-MM-dd HH:mm:ss.SSS 와 같이 입력할 수 있습니다. 미지정 시 _time 필드 값이 데이터 로딩 시점의 시각으로 결정됩니다.");
		setOptions("cs", OPTIONAL, "Encoding of text file. Default is 'utf-8'.", "텍스트 파일의 인코딩을 지정합니다. 미지정 시 기본값은 utf-8입니다.");

	}

	@Override
	public String getCommandName() {
		return "textfile";
	}

	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("10700", new QueryErrorMessage("invalid-textfile-path", "[file]이 존재하지 않거나 읽을수 없습니다."));
		return m;
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(), new ArrayList<String>(),
				getFunctionRegistry());
		@SuppressWarnings("unchecked")
		Map<String, String> options = (Map<String, String>) r.value;
		String filePath = commandString.substring(r.next).trim();

		try {
			long offset = 0;
			if (options.containsKey("offset"))
				offset = Long.valueOf(options.get("offset"));

			long limit = 0;
			if (options.containsKey("limit"))
				limit = Long.valueOf(options.get("limit"));

			String brex = null;
			if (options.containsKey("brex"))
				brex = options.get("brex");

			String erex = null;
			if (options.containsKey("erex"))
				erex = options.get("erex");

			String df = null;
			if (options.containsKey("df"))
				df = options.get("df");

			String dp = null;
			if (options.containsKey("dp"))
				dp = options.get("dp");

			String cs = "utf-8";
			if (options.containsKey("cs"))
				cs = options.get("cs");

			File f = new File(filePath);
			if (!f.exists() || !f.canRead()) {
				// throw new QueryParseException("invalid-textfile-path", -1);
				Map<String, String> params = new HashMap<String, String>();
				params.put("file", filePath);
				int offsetS = QueryTokenizer.findKeyword(commandString, filePath, getCommandName().length());
				throw new QueryParseException("10700", offsetS, offsetS + filePath.length() - 1, params);
			}

			String parserName = options.get("parser");
			LogParser parser = null;
			if (parserName != null) {
				LogParserFactory factory = parserFactoryRegistry.get(parserName);
				if (factory == null)
					throw new IllegalStateException("log parser not found: " + parserName);

				parser = factory.createParser(options);
			}

			return new TextFile(filePath, parser, offset, limit, brex, erex, df, dp, cs);
		} catch (QueryParseException t) {
			throw t;
		} catch (Throwable t) {
			throw new RuntimeException("cannot create textfile source", t);
		}
	}
}
