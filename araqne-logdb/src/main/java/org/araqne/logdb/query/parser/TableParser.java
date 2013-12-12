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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.araqne.log.api.LogParserFactoryRegistry;
import org.araqne.log.api.LogParserRegistry;
import org.araqne.log.api.WildcardMatcher;
import org.araqne.logdb.AccountService;
import org.araqne.logdb.LogMap;
import org.araqne.logdb.LogQueryCommand;
import org.araqne.logdb.LogQueryCommandParser;
import org.araqne.logdb.LogQueryContext;
import org.araqne.logdb.LogQueryParseException;
import org.araqne.logdb.Permission;
import org.araqne.logdb.query.command.Table;
import org.araqne.logdb.query.command.Table.TableParams;
import org.araqne.logdb.query.expr.Comma;
import org.araqne.logdb.query.expr.Expression;
import org.araqne.logdb.query.parser.ExpressionParser.FuncTerm;
import org.araqne.logdb.query.parser.ExpressionParser.TokenTerm;
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
		table.setAccountService(accountService);
		table.setTableRegistry(tableRegistry);
		table.setStorage(logStorage);
		table.setParserFactoryRegistry(parserFactoryRegistry);
		table.setParserRegistry(parserRegistry);
		return table;
	}

	private static enum OpTermI implements OpTerm {
		Comma(",", 200), ListEndComma(",", 200),
		NOP("", 0, true, false, true);

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
			if (token.startsWith("\"") && token.endsWith("\""))
				this.token = token.substring(1, token.length() - 1);
		}

		@Override
		public Object eval(LogMap map) {
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

	public static interface TableNameMatcher {
		String toString(String tableName);

		boolean matches(String tableName);
	}

	private static class TableSpec implements TableNameMatcher {
		String namespace;
		String table;
		Pattern pattern;

		public TableSpec(String spec) {
			Matcher matcher = TableSpec.qualifierPattern.matcher(spec);
			if (matcher.matches()) {
				namespace = matcher.group(1);
				table = matcher.group(2);
				pattern = WildcardMatcher.buildPattern(table);
			} else {
				throw new IllegalArgumentException();
			}
		}

		public static Pattern qualifierPattern = Pattern
				.compile("^(?:(`[^`]+`|[\\w\\*]+)\\:|)(`[^`]+`|[\\w\\*]+)");

		public String toString(String tableName) {
			if (namespace == null)
				return tableName;
			else
				return namespace + ":" + quote(tableName);
		}
		
		public static Pattern unquotedNameConstraint = Pattern.compile("^[\\w\\*]+$");

		private String quote(String tableName) {
			if (unquotedNameConstraint.matcher(tableName).matches()) {
				return tableName;
			} else {
				return "`" + tableName + "`";
			}
		}

		public boolean matches(String tableName) {
			if (pattern == null)
				return table.equals(tableName);
			else
				return pattern.matcher(tableName).matches();
		}
	}

	private static class Meta implements Expression {
		private ArrayList<TableNameMatcher> patterns;
		private MetadataMatcher mm;

		public Meta(List<Expression> args, LogTableRegistry tableRegistry) {
			Expression predicate = args.get(0);
			this.patterns = new ArrayList<TableNameMatcher>();
			for (Expression e : args.subList(1, args.size())) {
				try {
					patterns.add(new TableSpec(e.toString()));
				} catch (IllegalArgumentException exc) {
					throw new LogQueryParseException("invalid-table-spec", -1, e.toString());
				}
			}
			if (args.size() < 2) {
				patterns.add(new TableSpec("*"));
			}

			mm = new MetadataMatcher(predicate.eval(new LogMap()).toString(), tableRegistry, patterns);
		}

		@Override
		public Object eval(LogMap map) {
			LogMap m = new LogMap() {
				{
					put(MetadataMatcher.IDFACTORY_KEY, new MetadataMatcher.IdentifierFactory() {
						@Override
						public Object create(String concreteString) {
							return new String(concreteString);
						}
					});
				}
			};
			return mm.eval(m);
		}

	}

	private static class FuncEmitterFactoryI implements FuncEmitterFactory {
		private LogTableRegistry tableRegistry;

		public FuncEmitterFactoryI(LogTableRegistry tableRegistry) {
			this.tableRegistry = tableRegistry;
		}

		@Override
		public void emit(LogQueryContext context, Stack<Expression> exprStack, FuncTerm f) {
			String name = f.getName();
			List<Expression> args = getArgsFromStack(f, exprStack);

			if (name.equals("meta")) {
				exprStack.add(new Meta(args, tableRegistry));
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
	private List<String> parseTableNames(LogQueryContext context, String tableTokens) {
		List<String> tableNames = new ArrayList<String>();

		OpEmitterFactory of = new OpEmitterFactoryI();
		FuncEmitterFactory ff = new FuncEmitterFactoryI(tableRegistry);
		TermEmitterFactory tf = new TermEmitterFactoryI();

		Expression expr = ExpressionParser.parse(context, tableTokens, new ParsingRule(OpTermI.NOP, of, ff, tf));

		Object evalResult = expr.eval(new LogMap());
		if (evalResult instanceof List) {
		
			// TODO: can be more simplified
			for (Object o : (List<Object>) evalResult) {
				if (o instanceof List) {
					for (Object o2: (List<Object>) o) {
						String fqdn = o2.toString().trim();
						addTableNames(tableNames, context, fqdn);
					}
				} else {
					String fqdn = o.toString().trim();
					addTableNames(tableNames, context, fqdn);
				}
				
			}
		} else {
			addTableNames(tableNames, context, evalResult.toString());
		}
		
		if (tableNames.isEmpty())
			throw new LogQueryParseException("no-table-data-source", -1);

		return tableNames;
	}

	private void addTableNames(List<String> target, LogQueryContext context, String fqdn) {
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
				throw new LogQueryParseException("table-not-found", -1, "table=" + fqdn);

			if (!accountService.checkPermission(context.getSession(), name, Permission.READ))
				throw new LogQueryParseException("no-read-permission", -1, "table=" + fqdn);
		}

		target.add(fqdn);
	}
}
