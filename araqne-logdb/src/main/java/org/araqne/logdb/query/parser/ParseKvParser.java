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

import java.util.Arrays;
import java.util.Map;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryCommandParser;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.query.command.ParseKv;

public class ParseKvParser implements QueryCommandParser {

	@Override
	public String getCommandName() {
		return "parsekv";
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(),
				Arrays.asList("field", "overlay", "pairdelim", "kvdelim"));

		Map<String, String> options = (Map<String, String>) r.value;
		String field = options.get("field");
		if (field == null)
			field = "line";

		boolean overlay = CommandOptions.parseBoolean(options.get("overlay"));

		String pairDelim = options.get("pairdelim");
		if (pairDelim == null)
			pairDelim = " ";

		String kvDelim = options.get("kvdelim");
		if (kvDelim == null)
			kvDelim = "=";

		return new ParseKv(field, overlay, pairDelim, kvDelim);
	}
}
