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

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.Import;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogTableRegistry;

/**
 * @since 1.6.6
 * @author xeraph
 * 
 */
public class ImportParser extends AbstractQueryCommandParser {

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
	public QueryCommand parse(QueryContext context, String commandString) {
		if (context == null || !context.getSession().isAdmin())
			throw new QueryParseException("no-permission", -1);

		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(), Arrays.asList("create"),
				getFunctionRegistry());
		Map<String, String> m = (Map<String, String>) r.value;
		boolean create = CommandOptions.parseBoolean(m.get("create"));

		String tableName = commandString.substring(r.next).trim();
		if (!tableRegistry.exists(tableName) && !create)
			throw new QueryParseException("import-table-not-found", -1, tableName);

		return new Import(storage, tableName, create);
	}
}
