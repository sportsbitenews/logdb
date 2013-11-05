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

import org.araqne.logdb.LogQueryCommand;
import org.araqne.logdb.LogQueryCommandParser;
import org.araqne.logdb.LogQueryContext;
import org.araqne.logdb.LogQueryParseException;
import org.araqne.logdb.query.command.Import;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogTableRegistry;

/**
 * @since 1.6.6
 * @author xeraph
 * 
 */
public class ImportParser implements LogQueryCommandParser {

	private LogTableRegistry tableRegistry;
	private LogStorage storage;

	public ImportParser(LogTableRegistry tableRegistry, LogStorage storage) {
		this.tableRegistry = tableRegistry;
		this.storage = storage;
	}

	@Override
	public String getCommandName() {
		return "import";
	}

	@SuppressWarnings("unchecked")
	@Override
	public LogQueryCommand parse(LogQueryContext context, String commandString) {
		if (context == null || !context.getSession().isAdmin())
			throw new LogQueryParseException("no-permission", -1);

		boolean create = false;
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(), Arrays.asList("create"));
		Map<String, String> m = (Map<String, String>) r.value;
		if (m != null && m.containsKey("create"))
			create = Boolean.parseBoolean(m.get("create"));

		String tableName = commandString.substring(r.next).trim();
		if (!tableRegistry.exists(tableName) && !create)
			throw new LogQueryParseException("table-not-found", -1);

		return new Import(storage, tableName, create);
	}
}
