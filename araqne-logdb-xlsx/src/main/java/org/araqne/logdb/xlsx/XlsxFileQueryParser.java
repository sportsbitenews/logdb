/**
 * Copyright 2015 Eediom Inc.
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
package org.araqne.logdb.xlsx;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.FilePathHelper;
import org.araqne.logdb.LocalFilePathHelper;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.query.parser.ExpressionParser;
import org.araqne.logdb.query.parser.ParseResult;
import org.araqne.logdb.query.parser.QueryTokenizer;

@Component(name = "xlsxfile-query-parser")
public class XlsxFileQueryParser extends AbstractQueryCommandParser {

	@Requires
	private QueryParserService parserService;

	@Validate
	public void start() {
		parserService.addCommandParser(this);
	}

	@Invalidate
	public void stop() {
		if (parserService != null)
			parserService.removeCommandParser(this);
	}

	@Override
	public String getCommandName() {
		return "xlsxfile";
	}

	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("13000", new QueryErrorMessage("invalid-xlsxfile-path", "[file]이 존재하지 않거나 읽을수 없습니다."));
		m.put("13001", new QueryErrorMessage("invalid-parentfile-path", "[file]의 상위 디렉토리가 존재하지 않거나 읽을 수 없습니다."));
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
			long skip = 0;
			if (options.containsKey("skip"))
				skip = Long.valueOf(options.get("skip"));

			long offset = 0;
			if (options.containsKey("offset"))
				offset = Long.valueOf(options.get("offset"));

			long limit = Long.MAX_VALUE;
			if (options.containsKey("limit"))
				limit = Long.valueOf(options.get("limit"));

			String sheet = options.get("sheet");

			FilePathHelper pathHelper = new LocalFilePathHelper(filePath);

			return new XlsxFileQuery(pathHelper.getMatchedPaths(), filePath, sheet, offset, limit, skip);
		} catch (IllegalStateException e) {
			String msg = e.getMessage();
			Map<String, String> params = new HashMap<String, String>();
			params.put("file", filePath);
			int offsetS = QueryTokenizer.findKeyword(commandString, filePath, getCommandName().length());
			String type = null;
			if (msg.equals("file-not-found"))
				type = "13000";
			else
				type = "13001";

			throw new QueryParseException(type, offsetS, offsetS + filePath.length() - 1, params);
		} catch (QueryParseException t) {
			throw t;
		} catch (Throwable t) {
			throw new RuntimeException("cannot create xlsxfile source", t);
		}
	}
}
