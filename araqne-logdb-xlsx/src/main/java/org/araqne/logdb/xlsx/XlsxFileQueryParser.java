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
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParserService;
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
	public QueryCommand parse(QueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(), new ArrayList<String>(),
				getFunctionRegistry());
		@SuppressWarnings("unchecked")
		Map<String, String> options = (Map<String, String>) r.value;
		String filePath = commandString.substring(r.next).trim();

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

			return new XlsxFileQuery(filePath, sheet, offset, limit, skip);
		} catch (QueryParseException t) {
			throw t;
		} catch (Throwable t) {
			throw new RuntimeException("cannot create xlsxfile source", t);
		}
	}
}
