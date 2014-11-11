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
import java.util.Stack;

import org.araqne.log.api.LogParserFactoryRegistry;
import org.araqne.log.api.LogParserRegistry;
import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.AccountService;
import org.araqne.logdb.Permission;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
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
				Arrays.asList("from", "to", "offset", "limit", "duration", "parser", "order", "window", "raw"),
				getFunctionRegistry());
		Map<String, String> options = (Map<String, String>) r.value;
		String tableTokens = commandString.substring(r.next);
		List<TableSpec> tableNames = parseTableNames(context, tableTokens);

		Date from = null;
		Date to = null;
		long offset = 0;
		long limit = 0;
		String parser = null;
		boolean ordered = true;
		TimeSpan window = null;

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
			offset = Integer.parseInt(options.get("offset"));

		if (offset < 0)
			throw new QueryParseException("negative-offset", -1);

		if (options.containsKey("limit"))
			limit = Integer.parseInt(options.get("limit"));

		if (limit < 0)
			throw new QueryParseException("negative-limit", -1);

		if (options.containsKey("parser"))
			parser = options.get("parser");

		if (options.containsKey("order"))
			ordered = !options.get("order").equals("f");

		TableParams params = new TableParams();
		params.setTableSpecs(tableNames);
		params.setOffset(offset);
		params.setLimit(limit);
		params.setFrom(from);
		params.setTo(to);
		params.setParserName(parser);
		params.setOrdered(ordered);
		params.setWindow(window);
		params.setRaw(CommandOptions.parseBoolean(options.get("raw")));

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
					throw new QueryParseException("invalid-table-spec", -1, e.toString());
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

		if (tableNames.isEmpty())
			throw new QueryParseException("no-table-data-source", -1);

		return tableNames;
	}

	private void addTableSpec(List<TableSpec> target, QueryContext context, Object spec) {
		if (spec instanceof TableSpec) {
			target.add((TableSpec) spec);
		} else if (spec instanceof List) {
			for (Object o : (List<Object>) spec) {
				addTableSpec(target, context, o);
			}
		} else {
			WildcardTableSpec wspec = new WildcardTableSpec(spec.toString());
			if (!wspec.hasWildcard()) {
				List<StorageObjectName> sonList = wspec.match(tableRegistry);

				StorageObjectName son = sonList.get(0);

				if (son.getNamespace() == null) {
					// check only local tables
					if (!son.isOptional() && !tableRegistry.exists(son.getTable()))
						throw new QueryParseException("table-not-found", -1, "table=" + son.toString());

					if (!accountService.checkPermission(context.getSession(), son.getTable(), Permission.READ))
						throw new QueryParseException("no-read-permission", -1, "table=" + son.toString());
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
			if (!tableRegistry.exists(name))
				throw new QueryParseException("table-not-found", -1, "table=" + fqdn);

			if (!accountService.checkPermission(context.getSession(), name, Permission.READ))
				throw new QueryParseException("no-read-permission", -1, "table=" + fqdn);
		}

		target.add(fqdn);
	}
}
