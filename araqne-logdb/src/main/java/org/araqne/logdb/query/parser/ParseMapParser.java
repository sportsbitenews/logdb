package org.araqne.logdb.query.parser;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.ParseMap;

public class ParseMapParser extends AbstractQueryCommandParser {

	public ParseMapParser() {
		setDescriptions(
				"If specified field's value is map type, then assign new fields using key/value of map. Otherwise, pass original tuples.",
				"지정한 대상필드의 값이 맵 타입인 경우, 해당 맵의 모든 키/값을 새로운 필드로 할당합니다. 대상필드의 값이 null이거나 맵 타입이 아닌 경우, 원본 데이터를 그대로 전달합니다.");

		setOptions("field", REQUIRED, "Specify parsing target field which contains value of map type.",
				"맵을 포함하고 있는 파싱 대상 필드를 지정합니다.");
		setOptions("overlay", OPTIONAL, "Use 't' to overlay original input data with extracted fields. "
				+ "Otherwise, this command will pass only extracted fields.",
				"t로 주면, 원본 필드에 맵에서 추출된 필드를 덧씌운 결과를 출력으로 내보냅니다. 별도로 overlay 옵션을 지정하지 않으면, 맵을 파싱한 결과만 출력으로 내보냅니다.");
	}

	@Override
	public String getCommandName() {
		return "parsemap";
	}

	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("22100", new QueryErrorMessage("missing-parsemap-field", "필드를 입력하십시오."));
		return m;
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(),
				Arrays.asList("field", "overlay"), getFunctionRegistry());

		Map<String, String> options = (Map<String, String>) r.value;
		String field = options.get("field");
		if (field == null)
			// throw new QueryParseException("missing-parsemap-field", -1);
			throw new QueryParseException("22100", -1, -1, null);

		boolean overlay = CommandOptions.parseBoolean(options.get("overlay"));
		return new ParseMap(field, overlay);
	}

}
