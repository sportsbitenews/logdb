package org.araqne.logdb.query.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.LookupHandler;
import org.araqne.logdb.LookupHandlerRegistry;
import org.araqne.logdb.MemLookupHandler;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.MemLookup;

public class MemLookupParser extends AbstractQueryCommandParser {

	private LookupHandlerRegistry memLookupRegistry;

	public enum Op {
		LIST, BUILD, DROP;

		public static Op parse(String s) {
			for (Op op : values())
				if (op.name().toLowerCase().equals(s))
					return op;

			// throw new QueryParseException("unsupported-memlookup-op", -1);
			Map<String, String> params = new HashMap<String, String>();
			params.put("op", s);
			throw new QueryParseException("22000", -1, -1, params);
		}
	}

	public MemLookupParser(LookupHandlerRegistry memLookupRegistry) {
		this.memLookupRegistry = memLookupRegistry;

		setDescriptions(
				"Build instant memory lookup table using query. If no option is specified, memlookup will list instances with name, key, size.",
				"간편하게 lookup 명령어로 호출할 수 있는 인메모리 매핑테이블을 생성하려는 경우 memlookup 명령어를 활용합니다. memlookup 명령어에 아무런 옵션을 주지 않는 경우, memlookup 명령어로 생성된 모든 룩업의 메타데이터를 조회합니다. 아래와 같은 정보를 출력합니다:");

		setOptions("op", OPTIONAL, "list, drop, or build.", "list, drop, build 중 하나.");
		setOptions("name", OPTIONAL, "lookup name", "룩업 이름");
		setOptions("key", OPTIONAL, "lookup key field", "룩업 키 필드");
	}

	@Override
	public String getCommandName() {
		return "memlookup";
	}

	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("22000", new QueryErrorMessage("unsupported-memlookup-op", "[op]은 지원하지 않는 연산자 입니다."));
		m.put("22001", new QueryErrorMessage("missing-memlookup-name", "매핑 테이블 이름을 입력하십시오."));
		m.put("22002", new QueryErrorMessage("missing-memlookup-key", "매핑 테이블 키가 없습니다."));
		m.put("22003", new QueryErrorMessage("missing-memlookup-name", "매핑 테이블 이름을 입력하십시오."));
		m.put("22004", new QueryErrorMessage("invalid-memlookup-name", "[name]은 유효하지 않는 매핑 테이블 입니다."));
		m.put("22005", new QueryErrorMessage("invalid-memlookup-name", "[name]은 유효하지 않는 매핑 테이블 입니다."));
		return m;
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(),
				Arrays.asList("op", "name", "key"), getFunctionRegistry());

		@SuppressWarnings("unchecked")
		Map<String, String> opts = (Map<String, String>) r.value;

		Op op = Op.LIST;
		if (opts.get("op") != null) {
			op = Op.parse(opts.get("op"));
		}

		String name = opts.get("name");
		String key = opts.get("key");

		if (op == Op.BUILD) {
			if (name == null)
				// throw new QueryParseException("missing-memlookup-name", -1);
				throw new QueryParseException("22001", -1, -1, null);

			if (key == null)
				// throw new QueryParseException("missing-memlookup-key", -1);
				throw new QueryParseException("22002", -1, -1, null);

		} else if (op == Op.DROP && name == null) {
			// throw new QueryParseException("missing-memlookup-name", -1);
			throw new QueryParseException("22003", -1, -1, null);

		} else if (op == Op.LIST && name != null) {
			LookupHandler handler = memLookupRegistry.getLookupHandler(name);
			if (handler == null || !(handler instanceof MemLookupHandler)) {
				// throw new QueryParseException("invalid-memlookup-name", -1);
				Map<String, String> params = new HashMap<String, String>();
				params.put("name", name);
				throw new QueryParseException("22004", -1, -1, params);
			}
		}

		List<String> fields = new ArrayList<String>();
		StringTokenizer tok = new StringTokenizer(commandString.substring(r.next), ",");
		while (tok.hasMoreTokens())
			fields.add(tok.nextToken().trim());

		return new MemLookup(memLookupRegistry, op, name, key, fields);
	}
}
