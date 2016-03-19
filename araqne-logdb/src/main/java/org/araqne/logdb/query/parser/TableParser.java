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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.araqne.log.api.LogParserFactoryRegistry;
import org.araqne.log.api.LogParserRegistry;
import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.AccountService;
import org.araqne.logdb.Permission;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;
import org.araqne.logdb.TimeSpan;
import org.araqne.logdb.query.command.StorageObjectName;
import org.araqne.logdb.query.command.Table;
import org.araqne.logdb.query.command.Table.TableParams;
import org.araqne.logdb.query.expr.Comma;
import org.araqne.logdb.query.expr.Expression;
import org.araqne.logdb.query.parser.ExpressionParser.FuncTerm;
import org.araqne.logdb.query.parser.ExpressionParser.TokenTerm;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogStorageStatus;
import org.araqne.logstorage.LogTableRegistry;

public class TableParser extends AbstractQueryCommandParser {
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

		setDescriptions("Scan all tuples from tables.", "로그프레소 테이블에 저장된 데이터를 조회합니다.");
		setOptions("offset", false, "Skip count", "건너 뛸 로그 갯수");
		setOptions("limit", false, "Max output count", "가져올 최대 로그 갯수");
		setOptions("duration", false,
				"Scan only recent data. You should use s(second), m(minute), h(hour), d(day), mon(month) time unit. For example, `10s` means data from 10 seconds earlier.",
				"현재 시각으로부터 일정 시간 범위 이내의 로그로 한정. s(초), m(분), h(시), d(일), mon(월) 단위로 지정할 수 있습니다. 예를 들면, 10s의 경우 현재 시각으로부터 10초 이전까지의 범위를 의미합니다.");
		setOptions("from", false, "Start time of range. yyyyMMddHHmmss format. If you omit time part, it will be padded by zero.",
				"yyyyMMddHHmmss 포맷으로 범위의 시작을 지정합니다. 뒷자리를 쓰지 않으면 0으로 채워집니다.");
		setOptions("to", false, "End time of range. yyyyMMddHHmmss format. If you omit time part, it will be padded by zero.",
				"yyyyMMddHHmmss 포맷으로 범위의 끝을 지정합니다. 뒷자리를 쓰지 않으면 0으로 채워집니다.");
		setOptions("window", false,
				"Receive table input in realtime for specified duration. You should use s(second), m(minute), h(hour), d(day), mon(month) time unit. For example, `10s` means real-time data for 10 seconds timeout.",
				"쿼리 시작 시점으로부터 일정 시간 동안 테이블 입력을 수신합니다. s(초), m(분), h(시), d(일), mon(월) 단위로 지정할 수 있습니다. 예를 들면, 10s의 경우 쿼리 시작 후 10초 동안 입력을 수신합니다. window 옵션을 사용하는 경우 from, to, duration 옵션을 사용할 수 없습니다.");
	}

	@Override
	public String getCommandName() {
		return "table";
	}

	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("10600", new QueryErrorMessage("archive-not-opened", "저장소가 닫혀 있습니다."));
		m.put("10601", new QueryErrorMessage("negative-offset", "offset 값은 0보다 크거나 같아야 합니다: 입력값=[offset]."));
		m.put("10602", new QueryErrorMessage("negative-limit", "입력값이 허용 범위를 벗어났습니다: limit=[offset]."));
		m.put("10603", new QueryErrorMessage("invalid-table-spec", "[options]에서 [exp] 잘못된 옵션입니다."));
		m.put("10604", new QueryErrorMessage("no-table-data-source", "테이블이 없습니다."));
		m.put("10605", new QueryErrorMessage("table-not-found", "테이블 [table]이(가) 존재하지 않습니다."));
		m.put("10606", new QueryErrorMessage("no-read-permission", "테이블 [table] 읽기 권한이 없습니다."));
		m.put("10607", new QueryErrorMessage("table-not-found", "테이블 [table]이(가) 존재하지 않습니다."));
		m.put("10608", new QueryErrorMessage("no-read-permission", "테이블 [table] 읽기 권한이 없습니다."));
		return m;
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		if (logStorage.getStatus() != LogStorageStatus.Open)
			// throw new QueryParseException("archive-not-opened", -1);
			throw new QueryParseException("10600", -1, -1, null);

		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(), Arrays.asList("from", "to",
				"offset", "limit", "duration", "parser", "order", "window", "raw", "eachtable", "fields"), getFunctionRegistry());
		Map<String, String> options = (Map<String, String>) r.value;
		String tableTokens = commandString.substring(r.next);
		List<TableSpec> tableNames = parseTableNames(context, tableTokens);

		Date from = null;
		Date to = null;
		long offset = 0;
		long limit = 0;
		String parser = null;
		boolean ordered = true;
		boolean asc = false;
		TimeSpan window = null;
		boolean eachTable = false;

		if (options.containsKey("window"))
			window = TimeSpan.parse(options.get("window"));

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
			offset = Long.parseLong(options.get("offset"));

		if (offset < 0) {
			// throw new QueryParseException("negative-offset", -1);
			Map<String, String> param = new HashMap<String, String>();
			param.put("offset", options.get("offset"));
			int offsetS = QueryTokenizer.findKeyword(commandString, options.get("offset"));
			throw new QueryParseException("10601", offsetS, offsetS + options.get("offset").length() - 1, param);
		}

		if (options.containsKey("limit"))
			limit = Long.parseLong(options.get("limit"));

		if (limit < 0) {
			// throw new QueryParseException("negative-limit", -1);
			Map<String, String> param = new HashMap<String, String>();
			param.put("limit", options.get("limit"));
			int offsetS = QueryTokenizer.findKeyword(commandString, options.get("limit"));
			throw new QueryParseException("10602", offsetS, offsetS + options.get("limit").length() - 1, param);
		}

		if (options.get("parser") != null)
			parser = options.get("parser");

		String orderOpt = options.get("order");
		if (orderOpt != null) {
			ordered = !orderOpt.equals("f");
			asc = orderOpt.equals("asc");
		}

		eachTable = CommandOptions.parseBoolean(options.get("eachtable"));

		TableParams params = new TableParams();
		params.setTableSpecs(tableNames);
		params.setOffset(offset);
		params.setLimit(limit);
		params.setFrom(from);
		params.setTo(to);
		params.setParserName(parser);
		params.setOrdered(ordered);
		params.setAsc(asc);
		params.setWindow(window);
		params.setEachTable(eachTable);
		params.setRaw(CommandOptions.parseBoolean(options.get("raw")));

		// preserve field ordering
		if (options.get("fields") != null) {
			List<String> fields = new ArrayList<String>();
			for (String s : options.get("fields").split(",")) {
				s = s.trim();
				if (!s.isEmpty())
					fields.add(s);
			}
			params.setFields(fields);
		}

		if (params.isRaw()) {
			if (context.getSession() == null || !context.getSession().isAdmin())
				throw new QueryParseException("no-raw-permission", -1);
		}

		Table table = new Table(params);
		table.setAccountService(accountService);
		table.setTableRegistry(tableRegistry);
		table.setStorage(logStorage);
		table.setParserFactoryRegistry(parserFactoryRegistry);
		table.setParserRegistry(parserRegistry);
		return table;
	}

	private static enum OpTermI implements OpTerm {
		Comma(",", 200), ListEndComma(",", 200), NOP("", 0, true, false, true);

		private String symbol;
		private int precedence;
		private boolean leftAssoc;
		@SuppressWarnings("unused")
		private boolean unary;
		private boolean isAlpha;

		OpTermI(String symbol, int precedence) {
			this(symbol, precedence, true, false, false);
		}

		OpTermI(String symbol, int precedence, boolean leftAssoc, boolean unary, boolean isAlpha) {
			this.symbol = symbol;
			this.precedence = precedence;
			this.leftAssoc = leftAssoc;
			this.unary = unary;
			this.isAlpha = isAlpha;
		}

		@Override
		public String toString() {
			return symbol;
		}

		@Override
		public OpTerm parse(String token) {
			for (OpTermI op : values()) {
				if (op.symbol.equals(token) && op != NOP) {
					return op;
				}
			}
			return null;
		}

		@Override
		public boolean isInstance(Object o) {
			return o instanceof OpTermI;
		}

		@Override
		public boolean isUnary() {
			return false;
		}

		@Override
		public boolean isAlpha() {
			return isAlpha;
		}

		@Override
		public boolean isLeftAssoc() {
			return leftAssoc;
		}

		@Override
		public boolean isDelimiter(String s) {
			for (OpTermI op : values()) {
				if (!op.isAlpha && op.symbol.equals(s)) {
					return true;
				}
			}
			return false;
		}

		@Override
		public String getSymbol() {
			return symbol;
		}

		@Override
		public int getPrecedence() {
			return precedence;
		}

		@Override
		public List<OpTerm> delimiters() {
			List<OpTerm> delims = new ArrayList<OpTerm>();
			for (OpTermI op : values()) {
				if (!op.isAlpha()) {
					delims.add(op);
				}
			}

			return delims;
		}

		@Override
		public OpTerm postProcessCloseParen() {
			if (!this.equals(Comma)) {
				return this;
			}

			return ListEndComma;
		}

		@Override
		public boolean hasAltOp() {
			return false;
		}

		@Override
		public OpTerm getAltOp() {
			return null;
		}
	}

	private static class OpEmitterFactoryI implements OpEmitterFactory {
		@Override
		public void emit(Stack<Expression> exprStack, Term term) {
			OpTermI op = (OpTermI) term;

			Expression rhs = exprStack.pop();
			Expression lhs = exprStack.pop();

			switch (op) {
			case Comma:
				exprStack.add(new Comma(lhs, rhs));
				break;
			case ListEndComma:
				exprStack.add(new Comma(lhs, rhs, true));
				break;

			default:
				throw new IllegalStateException("unsupported operator " + op.toString());
			}
		}
	};

	private static class StringConstant implements Expression {
		private String token;

		public StringConstant(String token) {
			this.token = token;
		}

		@Override
		public Object eval(Row map) {
			if (token.startsWith("\"") && token.endsWith("\""))
				return token.substring(1, token.length() - 1);
			else
				return token.toString();
		}

		public String toString() {
			return token;
		}
	}

	private static class TermEmitterFactoryI implements TermEmitterFactory {
		@Override
		public void emit(Stack<Expression> exprStack, TokenTerm t) {
			exprStack.add(new StringConstant(t.toString()));
		}
	}

	private static class MetaS implements TableSpec {
		private Expression predicate;
		private TableSpec pattern;
		private MetadataMatcher<TableSpec> mm;

		public MetaS(Expression pred, TableSpec pat) {
			this.predicate = pred;
			this.pattern = pat;
			this.mm = new MetadataMatcher<TableSpec>(predicate.eval(new Row()).toString(), Arrays.asList(pattern));
		}

		public Object clone() {
			return new MetaS(predicate, pattern);
		}

		@Override
		public String getSpec() {
			return toString();
		}

		@Override
		public List<StorageObjectName> match(LogTableRegistry logTableRegistry) {
			return mm.match(logTableRegistry);
		}

		@Override
		public String getNamespace() {
			return pattern.getNamespace();
		}

		@Override
		public void setNamespace(String ns) {
			pattern.setNamespace(ns);
		}

		@Override
		public String getTable() {
			return pattern.getTable();
		}

		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("meta(");
			sb.append(predicate.toString());
			sb.append(", " + pattern.toString());
			sb.append(")");
			return sb.toString();
		}

		@Override
		public boolean isOptional() {
			return true;
		}

		@Override
		public void setOptional(boolean optional) {
			throw new UnsupportedOperationException("set table-metadata matcher as not-optional is not supported");
		}
	}

	private static class Meta implements Expression {
		private List<TableSpec> patterns;
		private MetadataMatcher<TableSpec> mm;
		private String predStr;
		private List<Expression> args;

		public Meta(List<Expression> args) {
			this.args = args;
			Expression predicate = args.get(0);
			this.predStr = predicate.toString();
			this.patterns = new ArrayList<TableSpec>();
			for (Expression e : args.subList(1, args.size())) {
				try {
					this.patterns.add(new WildcardTableSpec(e.eval(new Row()).toString()));
				} catch (IllegalArgumentException exc) {
					// throw new QueryParseException("invalid-table-spec", -1,
					// e.toString());

					Map<String, String> param = new HashMap<String, String>();
					try {
						param.put("options", args.toString());
						param.put("exp", e.toString());
					} catch (Throwable t) {
					}
					throw new QueryParseException("10603", -1, -1, param);
				}
			}
			if (args.size() < 2) {
				this.patterns.add(new WildcardTableSpec("*"));
			}

			this.mm = new MetadataMatcher<TableSpec>(predicate.eval(new Row()).toString(), patterns);
		}

		public Object clone() {
			return new Meta(args);
		}

		@Override
		public Object eval(Row map) {
			List<TableSpec> result = new ArrayList<TableSpec>();
			for (TableSpec ts : patterns) {
				result.add(new MetaS(args.get(0), ts));
			}
			return result;
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("meta(");
			sb.append(predStr);
			for (TableSpec spec : patterns) {
				sb.append(", " + spec.toString());
			}
			sb.append(")");
			return sb.toString();
		}
	}

	private static class FuncEmitterFactoryI implements FuncEmitterFactory {
		public FuncEmitterFactoryI() {
		}

		@Override
		public void emit(QueryContext context, Stack<Expression> exprStack, FuncTerm f) {
			String name = f.getName();
			List<Expression> args = getArgsFromStack(f, exprStack);

			if (name.equals("meta")) {
				exprStack.add(new Meta(args));
			}
		}

		private List<Expression> getArgsFromStack(FuncTerm f, Stack<Expression> exprStack) {
			List<Expression> exprs = null;
			if (exprStack.isEmpty() || !f.hasArgument())
				return new ArrayList<Expression>();

			Expression arg = exprStack.pop();
			if (arg instanceof Comma) {
				exprs = ((Comma) arg).getList();
			} else {
				exprs = new ArrayList<Expression>();
				exprs.add(arg);
			}
			return exprs;
		}
	}

	@SuppressWarnings("unchecked")
	private List<TableSpec> parseTableNames(QueryContext context, String tableTokens) {
		List<TableSpec> tableNames = new ArrayList<TableSpec>();

		OpEmitterFactory of = new OpEmitterFactoryI();
		FuncEmitterFactory ff = new FuncEmitterFactoryI();
		TermEmitterFactory tf = new TermEmitterFactoryI();

		Expression expr = ExpressionParser.parse(context, tableTokens, new ParsingRule(OpTermI.NOP, of, ff, tf));

		Object evalResult = expr.eval(new Row());
		if (evalResult instanceof List) {
			for (Object o : (List<Object>) evalResult) {
				addTableSpec(tableNames, context, o);
			}
		} else {
			addTableSpec(tableNames, context, evalResult);
		}

		if (tableNames.isEmpty()) {
			// throw new QueryParseException("no-table-data-source", -1);
			Map<String, String> params = new HashMap<String, String>();
			params.put("value", tableTokens);
			throw new QueryParseException("10604", -1, -1, params);
		}
		return tableNames;
	}

	private void addTableSpec(List<TableSpec> target, QueryContext context, Object spec) {
		if (spec instanceof TableSpec) {
			target.add((TableSpec) spec);
		} else if (spec instanceof List) {
			@SuppressWarnings("unchecked")
			List<Object> specs = (List<Object>) spec;
			for (Object o : specs) {
				addTableSpec(target, context, o);
			}
		} else {
			WildcardTableSpec wspec = new WildcardTableSpec(spec.toString());
			if (!wspec.hasWildcard()) {
				List<StorageObjectName> sonList = wspec.match(tableRegistry);
				StorageObjectName son = sonList.get(0);

				if (son.getNamespace() == null) {
					// check only local tables
					if (!son.isOptional() && !tableRegistry.exists(son.getTable())) {
						// throw new QueryParseException("table-not-found", -1,
						// "table=" + son.toString());
						String table = son.toString();
						Map<String, String> param = new HashMap<String, String>();
						param.put("table", table);
						throw new QueryParseException("10605", -1, -1, param);
					}
					if (!accountService.checkPermission(context.getSession(), son.getTable(), Permission.READ)) {
						// throw new QueryParseException("no-read-permission",
						// -1, "table=" + son.toString());
						String table = son.toString();
						Map<String, String> param = new HashMap<String, String>();
						param.put("table", table);
						throw new QueryParseException("10606", -1, -1, param);
					}
				}
			}
			target.add(wspec);
		}
	}

	private void addTableNames(List<String> target, QueryContext context, String fqdn) {
		// strip namespace
		String name = fqdn;
		String namespace = null;
		int pos = fqdn.lastIndexOf(':');
		if (pos >= 0) {
			namespace = fqdn.substring(0, pos);
			name = fqdn.substring(pos + 1);
			// XXX
			if (name.startsWith("`") && name.endsWith("`")) {
				name = name.substring(1, name.length() - 1);
				fqdn = namespace + ":" + name;
			}
		} else {
			// XXX
			if (name.startsWith("`") && name.endsWith("`")) {
				name = name.substring(1, name.length() - 1);
				fqdn = name;
			}
		}

		if (namespace == null && !name.contains("*")) {
			// check only local tables
			if (!tableRegistry.exists(name)) {
				// throw new QueryParseException("table-not-found", -1, "table="
				// + fqdn);
				Map<String, String> params = new HashMap<String, String>();
				params.put("table", fqdn);
				throw new QueryParseException("10607", -1, -1, params);
			}

			if (!accountService.checkPermission(context.getSession(), name, Permission.READ)) {
				// throw new QueryParseException("no-read-permission", -1,
				// "table=" + fqdn);
				Map<String, String> params = new HashMap<String, String>();
				params.put("table", fqdn);
				throw new QueryParseException("10608", -1, -1, params);
			}
		}

		target.add(fqdn);
	}
}
