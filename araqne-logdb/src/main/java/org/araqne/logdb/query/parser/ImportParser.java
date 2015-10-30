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
import java.util.HashMap;
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
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

		setDescriptions("Write all tuples to the table. This command requires administrator privilege.",
				"입력되는 모든 레코드를 지정된 테이블에 기록합니다. 관리자 권한이 없으면 쿼리가 실패합니다.");

		setOptions("create", false,
				"Create new table if table does not exist. If table does not exist and create option is false, then query will fail.",
				"t로 지정되는 경우 테이블이 없으면 자동으로 테이블을 생성합니다. create 옵션이 지정되지 않은 상태에서 임포트할 테이블이 존재하지 않으면 쿼리가 실패합니다.");
	}

	@Override
	public String getCommandName() {
		return "import";
	}

	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("30100", new QueryErrorMessage("no-permission", "권한이 없습니다. 관리자 권한이 필요합니다."));
		m.put("30101", new QueryErrorMessage("import-table-not-found", "[table]은 존재하지 않는 테이블입니다."));
		return m;
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		if (context == null || !context.getSession().isAdmin())
			// throw new QueryParseException("no-permission", -1);
			throw new QueryParseException("30100", -1, -1, null);

		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(), Arrays.asList("create"),
				getFunctionRegistry());
		Map<String, String> m = (Map<String, String>) r.value;
		boolean create = CommandOptions.parseBoolean(m.get("create"));

		String tableName = commandString.substring(r.next).trim();
		if (!tableRegistry.exists(tableName) && !create) {
			// throw new QueryParseException("import-table-not-found", -1,
			// tableName);
			Map<String, String> params = new HashMap<String, String>();
			params.put("table", tableName);
			throw new QueryParseException("30101", r.next, commandString.length() - 1, params);
		}

		return new Import(storage, tableName, create);
	}
}
