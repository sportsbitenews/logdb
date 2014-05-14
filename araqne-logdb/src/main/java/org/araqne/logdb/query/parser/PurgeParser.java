/**
 * Copyright 2014 Eediom Inc.
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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.Purge;
import org.araqne.logdb.query.command.StorageObjectName;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogTableRegistry;

/**
 * @since 2.2.10
 * 
 * @author darkluster
 * 
 */
public class PurgeParser extends AbstractQueryCommandParser {
	private LogStorage storage;
	private LogTableRegistry tableRegistry;

	public PurgeParser(LogStorage storage, LogTableRegistry tableRegistry) {
		this.storage = storage;
		this.tableRegistry = tableRegistry;
	}

	@Override
	public String getCommandName() {
		return "purge";
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		if (!context.getSession().isAdmin())
			throw new QueryParseException("no-permission", -1);

		if (commandString.trim().endsWith(","))
			throw new QueryParseException("missing-field", commandString.length());

		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(),
				Arrays.asList("from", "to"), getFunctionRegistry());
		Map<String, String> options = (Map<String, String>) r.value;
		if (options.get("from") == null)
			throw new QueryParseException("from-not-found", -1);

		if (options.get("to") == null)
			throw new QueryParseException("to-not-found", -1);

		String fromString = options.get("from").toString();
		String toString = options.get("to").toString();

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		try {
			Date from = sdf.parse(fromString);
			Date to = sdf.parse(toString);

			String tableName = commandString.substring(r.next).trim();

			return new Purge(storage, expandTableNames(tableName), from, to);
		} catch (ParseException e) {
			throw new QueryParseException("invalid-date-format", -1);
		}
	}

	private List<String> expandTableNames(String tableName) {
		List<String> localTableNames = new ArrayList<String>();
		for (String splitedTableName : tableName.split(",")) {
			WildcardTableSpec tableMatcher = new WildcardTableSpec(splitedTableName);
			for (StorageObjectName son : tableMatcher.match(tableRegistry)) {
				if (son.getNamespace() != null && !son.getNamespace().equals("*"))
					continue;
				if (son.isOptional() && !tableRegistry.exists(son.getTable()))
					continue;
				if (!son.isOptional() && !tableRegistry.exists(son.getTable()))
					throw new QueryParseException("table-not-found", -1);
				localTableNames.add(son.getTable());
			}
		}
		return localTableNames;
	}
}
