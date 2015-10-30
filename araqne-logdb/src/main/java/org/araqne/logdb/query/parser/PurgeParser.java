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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
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

		setDescriptions("Purge specified range of data when query starts. This command requires administrator privilege.",
				"전체 쿼리가 시작하는 시점에 지정한 날짜 범위의 테이블 데이터를 파기합니다. 주로 쿼리를 실행할 때마다 기존 데이터를 파기하고 새로운 데이터를 입력하려는 경우에 사용합니다. 이 커맨드를 실행하려면 관리자 권한이 필요합니다.");
		setOptions("from", true, "yyyyMMdd format. Start day of range", "yyyyMMdd 포맷, 파기 대상 시작 날짜");
		setOptions("to", true, "yyyyMMdd format. End day of range", "yyyyMMdd 포맷, 파기 대상 끝 날짜");
	}

	@Override
	public String getCommandName() {
		return "purge";
	}

	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("11400", new QueryErrorMessage("no-permission", "프로시저를 찾을 수 없습니다."));
		m.put("11401", new QueryErrorMessage("missing-field", "불완전한 표현식입니다."));
		m.put("11402", new QueryErrorMessage("from-not-found", "시작 날짜를 입력하십시오."));
		m.put("11403", new QueryErrorMessage("to-not-found", "끝 날짜를 입력하십시오."));
		m.put("11404", new QueryErrorMessage("invalid-date-format", "잘못된 날짜표현식입니다: [msg]."));
		m.put("11405", new QueryErrorMessage("table-not-found", "[table] 테이블이 존재하지 않습니다."));
		m.put("11406", new QueryErrorMessage("invalid-date-range", "유효하지 않은 날짜 범위 입니다."));
		return m;
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		if (!context.getSession().isAdmin())
			// throw new QueryParseException("no-permission", -1);
			throw new QueryParseException("11400", -1, -1, null);

		if (commandString.trim().endsWith(","))
			// throw new QueryParseException("missing-field",
			// commandString.length());
			throw new QueryParseException("11401", -1, -1, null);

		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(),
				Arrays.asList("from", "to"), getFunctionRegistry());
		Map<String, String> options = (Map<String, String>) r.value;
		if (options.get("from") == null)
			// throw new QueryParseException("from-not-found", -1);
			throw new QueryParseException("11402", -1, -1, null);

		if (options.get("to") == null)
			// throw new QueryParseException("to-not-found", -1);
			throw new QueryParseException("11403", -1, -1, null);

		String fromString = options.get("from").toString();
		String toString = options.get("to").toString();

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		try {
			Date from = sdf.parse(fromString);
			Date to = sdf.parse(toString);

			if (from.after(to))
				throw new QueryParseException("11406", -1, -1, null);

			String tableName = ExpressionParser.evalContextReference(context, commandString.substring(r.next).trim(),
					getFunctionRegistry());

			return new Purge(storage, expandTableNames(tableName), from, to);
		} catch (ParseException e) {
			// throw new QueryParseException("invalid-date-format", -1);
			Map<String, String> params = new HashMap<String, String>();
			params.put("msg", e.getMessage());
			throw new QueryParseException("11404", -1, -1, params);
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
				if (!son.isOptional() && !tableRegistry.exists(son.getTable())) {
					// hrow new QueryParseException("table-not-found", -1);
					Map<String, String> params = new HashMap<String, String>();
					params.put("table", tableName);
					throw new QueryParseException("11405", -1, -1, params);
				}
				localTableNames.add(son.getTable());
			}
		}
		return localTableNames;
	}
}
