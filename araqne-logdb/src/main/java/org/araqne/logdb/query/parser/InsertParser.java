package org.araqne.logdb.query.parser;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.Insert;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogTableRegistry;

/**
 * 
 * @author darkluster
 * 
 * @since
 * 
 */
public class InsertParser extends AbstractQueryCommandParser {
	private LogTableRegistry tableRegistry;
	private LogStorage storage;

	public InsertParser(LogTableRegistry tableRegistry, LogStorage storage) {
		this.tableRegistry = tableRegistry;
		this.storage = storage;

		setDescriptions(
				"Insert tuple to the table which is specified by field value. This command requires administrator privilege.",
				"입력된 필드 값을 기준으로 테이블을 선택하여 데이터를 입력합니다. 관리자 권한이 없으면 쿼리가 실패합니다.");

		setOptions("table", true, "Field which has name of target table.", "대상 테이블을 지정할 필드 이름");
		setOptions("create", false,
				"Create new table if table does not exist. If table does not exist and create option is false, then query will fail.",
				"t로 지정되는 경우 테이블이 없으면 자동으로 테이블을 생성합니다. create 옵션이 지정되지 않은 상태에서 임포트할 테이블이 존재하지 않으면 쿼리가 중단됩니다.");
	}

	@Override
	public String getCommandName() {
		return "insert";
	}

	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("30700", new QueryErrorMessage("no-permission", "권한이 없습니다. 관리자 권한이 필요합니다."));
		m.put("30701", new QueryErrorMessage("missing-field", "필드이름을 입력하십시오."));
		return m;
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		if (context == null || !context.getSession().isAdmin())
			// throw new QueryParseException("no-permission", -1);
			throw new QueryParseException("30700", -1, -1, null);

		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(),
				Arrays.asList("table", "create"), getFunctionRegistry());

		@SuppressWarnings("unchecked")
		Map<String, String> options = (Map<String, String>) r.value;
		String tableNameField = options.get("table");
		if (tableNameField == null)
			// throw new QueryParseException("missing-field",
			// commandString.length());
			throw new QueryParseException("30701", -1, -1, null);
		boolean create = CommandOptions.parseBoolean(options.get("create"));

		return new Insert(tableRegistry, storage, tableNameField, create);
	}

}
