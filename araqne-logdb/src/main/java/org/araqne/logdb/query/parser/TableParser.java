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
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.araqne.log.api.LogParserFactoryRegistry;
import org.araqne.log.api.LogParserRegistry;
import org.araqne.log.api.WildcardMatcher;
import org.araqne.logdb.AccountService;
import org.araqne.logdb.LogQueryCommand;
import org.araqne.logdb.LogQueryCommandParser;
import org.araqne.logdb.LogQueryContext;
import org.araqne.logdb.LogQueryParseException;
import org.araqne.logdb.Permission;
import org.araqne.logdb.query.command.Table;
import org.araqne.logdb.query.command.Table.TableParams;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogStorageStatus;
import org.araqne.logstorage.LogTableRegistry;

public class TableParser implements LogQueryCommandParser {
	private AccountService accountService;
	private LogStorage logStorage;
	private LogTableRegistry tableRegistry;
	private LogParserFactoryRegistry parserFactoryRegistry;
	private LogParserRegistry parserRegistry;

	public TableParser(AccountService accountService, LogStorage logStorage, LogTableRegistry tableRegistry,
			LogParserFactoryRegistry parserFactoryRegistry, LogParserRegistry parserRegistry) {
		this.accountService = accountService;
		this.logStorage = logStorage;
		this.tableRegistry = tableRegistry;
		this.parserFactoryRegistry = parserFactoryRegistry;
		this.parserRegistry = parserRegistry;
	}

	@Override
	public String getCommandName() {
		return "table";
	}

	@SuppressWarnings("unchecked")
	@Override
	public LogQueryCommand parse(LogQueryContext context, String commandString) {
		if (logStorage.getStatus() != LogStorageStatus.Open)
			throw new LogQueryParseException("archive-not-opened", -1);

		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(),
				Arrays.asList("from", "to", "offset", "limit", "duration", "parser"));
		Map<String, String> options = (Map<String, String>) r.value;
		String tableTokens = commandString.substring(r.next);
		List<String> tableNames = parseTableNames(context, tableTokens);

		Date from = null;
		Date to = null;
		long offset = 0;
		long limit = 0;
		String parser = null;

		if (options.containsKey("duration")) {
			String duration = options.get("duration");
			int i;
			for (i = 0; i < duration.length(); i++) {
				char c = duration.charAt(i);
				if (!('0' <= c && c <= '9'))
					break;
			}
			int value = Integer.parseInt(duration.substring(0, i));
			from = QueryTokenizer.getDuration(value, duration.substring(i));
		}

		if (options.containsKey("from"))
			from = QueryTokenizer.getDate(options.get("from"));

		if (options.containsKey("to"))
			to = QueryTokenizer.getDate(options.get("to"));

		if (options.containsKey("offset"))
			offset = Integer.parseInt(options.get("offset"));

		if (offset < 0)
			throw new LogQueryParseException("negative-offset", -1);

		if (options.containsKey("limit"))
			limit = Integer.parseInt(options.get("limit"));

		if (limit < 0)
			throw new LogQueryParseException("negative-limit", -1);

		if (options.containsKey("parser"))
			parser = options.get("parser");

		TableParams params = new TableParams();
		params.setTableNames(tableNames);
		params.setOffset(offset);
		params.setLimit(limit);
		params.setFrom(from);
		params.setTo(to);
		params.setParserName(parser);

		Table table = new Table(params);
		table.setTableRegistry(tableRegistry);
		table.setStorage(logStorage);
		table.setParserFactoryRegistry(parserFactoryRegistry);
		table.setParserRegistry(parserRegistry);
		return table;
	}

	private List<String> parseTableNames(LogQueryContext context, String tableTokens) {
		List<String> tableNames = new ArrayList<String>();
		for (String tableNameToken : tableTokens.split(",")) {
			String tableName = tableNameToken.trim();

			if (tableName.contains("*")) {
				Pattern p = WildcardMatcher.buildPattern(tableName);
				addWildMatchTables(context, tableNames, p.matcher(""));
			} else {
				if (!tableRegistry.exists(tableName))
					throw new LogQueryParseException("table-not-found", -1, "table=" + tableName);

				if (!accountService.checkPermission(context.getSession(), tableName, Permission.READ))
					throw new LogQueryParseException("no-read-permission", -1, "table=" + tableName);

				tableNames.add(tableName);
			}
		}
		return tableNames;
	}

	private void addWildMatchTables(LogQueryContext context, List<String> tableNames, Matcher matcher) {
		for (String tableName : tableRegistry.getTableNames()) {
			matcher.reset(tableName);
			if (!matcher.matches())
				continue;

			if (!accountService.checkPermission(context.getSession(), tableName, Permission.READ))
				continue;

			tableNames.add(tableName);
		}
	}
}
