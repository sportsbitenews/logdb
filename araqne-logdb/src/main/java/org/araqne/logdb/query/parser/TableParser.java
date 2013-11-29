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

import org.araqne.log.api.LogParserFactoryRegistry;
import org.araqne.log.api.LogParserRegistry;
import org.araqne.logdb.AccountService;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Permission;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryCommandParser;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.query.command.Table;
import org.araqne.logdb.query.command.Table.TableParams;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogStorageStatus;
import org.araqne.logstorage.LogTableRegistry;

public class TableParser implements QueryCommandParser {
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
	public QueryCommand parse(QueryContext context, String commandString) {
		if (logStorage.getStatus() != LogStorageStatus.Open)
			throw new QueryParseException("archive-not-opened", -1);

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
			throw new QueryParseException("negative-offset", -1);

		if (options.containsKey("limit"))
			limit = Integer.parseInt(options.get("limit"));

		if (limit < 0)
			throw new QueryParseException("negative-limit", -1);

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
		table.setAccountService(accountService);
		table.setTableRegistry(tableRegistry);
		table.setStorage(logStorage);
		table.setParserFactoryRegistry(parserFactoryRegistry);
		table.setParserRegistry(parserRegistry);
		return table;
	}

	private List<String> parseTableNames(QueryContext context, String tableTokens) {
		List<String> tableNames = new ArrayList<String>();
		for (String tableNameToken : tableTokens.split(",")) {
			String fqdn = tableNameToken.trim();

			// strip namespace
			String name = fqdn;
			String namespace = null;
			int pos = fqdn.lastIndexOf(':');
			if (pos >= 0) {
				namespace = fqdn.substring(0, pos);
				name = fqdn.substring(pos + 1);
			}

			if (namespace == null && !name.contains("*")) {
				// check only local tables
				if (!tableRegistry.exists(name))
					throw new QueryParseException("table-not-found", -1, "table=" + fqdn);

				if (!accountService.checkPermission(context.getSession(), name, Permission.READ))
					throw new QueryParseException("no-read-permission", -1, "table=" + fqdn);
			}

			tableNames.add(fqdn);
		}
		return tableNames;
	}
}
