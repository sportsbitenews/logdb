package org.araqne.logdb.query.command;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.araqne.ahocorasick.AhoCorasickSearch;
import org.araqne.ahocorasick.SearchResult;
import org.araqne.logdb.Query;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.QueryTask;
import org.araqne.logdb.Row;
import org.araqne.logdb.Strings;
import org.araqne.logdb.query.expr.And;
import org.araqne.logdb.query.expr.Expression;
import org.araqne.logdb.query.expr.Or;
import org.araqne.logdb.query.parser.ExpressionParser;
import org.araqne.logdb.query.parser.ExpressionParser.FuncTerm;
import org.araqne.logdb.query.parser.ExpressionParser.TokenTerm;
import org.araqne.logdb.query.parser.FuncEmitterFactory;
import org.araqne.logdb.query.parser.OpEmitterFactory;
import org.araqne.logdb.query.parser.OpTerm;
import org.araqne.logdb.query.parser.ParsingRule;
import org.araqne.logdb.query.parser.Term;
import org.araqne.logdb.query.parser.TermEmitterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AcSearch extends QueryCommand {
	private final Logger logger = LoggerFactory.getLogger(AcSearch.class);

	private AhoCorasickSearch acs;
	private String field;
	private Query subQuery;

	private Map<Expression, Object> exprmap;

	private AtomicInteger subQueryCounter;
	private CountDownLatch subQueryLatch;
	private CountDownLatch subQueryTaskLatch;

	public AcSearch(String field, Query subQuery, CountDownLatch subQueryLatch) {
		this.field = field;
		this.subQuery = subQuery;
		this.subQueryLatch = subQueryLatch;

		this.subQueryCounter = new AtomicInteger(0);
		this.subQueryTaskLatch = new CountDownLatch(1);
	}

	@Override
	public String getName() {
		return "acsearch";
	}

	@Override
	public void onStart() {
		subQuery.preRun();
	}

	@Override
	protected void onClose(QueryStopReason reason) {
		try {
			subQuery.stop(reason);
		} catch (Throwable t) {
			logger.error("araqne logdb: cannot stop subquery [" + subQuery.getQueryString() + "]", t);
		} finally {
			subQuery.purge();
		}
	}

	private void waitForSubQueryTaskDone() {
		if (subQueryCounter.compareAndSet(0, 1)) {
			SubQueryTask subQueryTask = new SubQueryTask();
			subQueryTask.run();
			subQueryTaskLatch.countDown();
		} else {
			try {
				subQueryTaskLatch.await();
			} catch (InterruptedException e) {
			}
		}
	}

	@Override
	public void onPush(Row row) {
		waitForSubQueryTaskDone();

		Map<String, Object> m = row.map();
		if (!row.containsKey(field) || row.get(field) == null) {
			pushPipe(new Row(m));
			return;
		}

		String content = m.get(field).toString();
		List<SearchResult> acsResults = acs.search(content);
		Set<String> found = new HashSet<String>();
		for (SearchResult sr : acsResults)
			found.add(sr.getKeyword().getName());
		Map<String, Object> result = new HashMap<String, Object>();
		result.put("result", found);

		List<Object> l = new ArrayList<Object>();
		for (Expression e : exprmap.keySet()) {
			if (e.eval(new Row(result)) == Boolean.TRUE) {
				l.add(exprmap.get(e));
			}
		}

		m.put("_ac_result", l);
		pushPipe(new Row(m));
	}

	private class SubQueryTask extends QueryTask {
		@Override
		public void run() {
			if (logger.isDebugEnabled())
				logger.debug("logpresso query: acsearch sub query task run [{}]", subQuery.getQueryString());

			try {
				subQuery.run();
				subQueryLatch.await();

				List<Map<String, Object>> subQueryResult = subQuery.getResultAsList();
				Set<String> keywords = new HashSet<String>();
				exprmap = new HashMap<Expression, Object>();
				for (Map<String, Object> m : subQueryResult) {
					Object e = m.get("expr");
					if (e == null)
						continue;

					try {
						OpEmitterFactory of = new AcSearchOpEmitterFactory();
						TermEmitterFactory tf = new AcSearchTermEmitterFactory(keywords);
						FuncEmitterFactory ff = new AcSearchFuncEmitterFactory();
						ParsingRule pr = new ParsingRule(AcSearchOpTerm.NOP, of, ff, tf);

						Expression expr = ExpressionParser.parse(getContext(), e.toString(), pr);
						exprmap.put(expr, m);
					} catch (QueryParseException q) {
						logger.warn("logpresso query: invalid expr content [" + e.toString() + "]");
					}
				}

				acs = new AhoCorasickSearch();
				for (String keyword : keywords)
					acs.addKeyword(keyword);
				acs.compile();
			} catch (Throwable t) {
				logger.error("logpresso query: acsearch subquery error", t);
			} finally {
			}
		}
	}

	private class AcSearchOpEmitterFactory implements OpEmitterFactory {
		@Override
		public void emit(Stack<Expression> exprStack, Term term) {
			AcSearchOpTerm op = (AcSearchOpTerm) term;

			if (op.isUnary()) {
				Expression expr = exprStack.pop();
				if (op != AcSearchOpTerm.Not)
					throw new QueryParseException("unsupported operator " + op.toString(), -1);
				exprStack.add(new NotExpression(expr));
				return;
			}

			if (exprStack.size() < 2)
				throw new QueryParseException("broken-expression", -1, "operator is [" + op + "]");

			Expression rhs = exprStack.pop();
			Expression lhs = exprStack.pop();
			switch (op) {
			case And:
				exprStack.add(new And(lhs, rhs));
				break;

			case Or:
				exprStack.add(new Or(lhs, rhs));
				break;

			default:
				throw new QueryParseException("unsupported operator " + op.toString(), -1);
			}
		}
	}

	private class NotExpression implements Expression {
		private Expression expr;

		public NotExpression(Expression expr) {
			this.expr = expr;
		}

		@Override
		public Object eval(Row row) {
			return !(expr.eval(row) == Boolean.TRUE);
		}
	}

	private class AcSearchTermEmitterFactory implements TermEmitterFactory {
		private Set<String> keywords;

		public AcSearchTermEmitterFactory(Set<String> keywords) {
			this.keywords = keywords;
		}

		@Override
		public void emit(Stack<Expression> exprStack, TokenTerm t) {
			if (!t.getText().equals("(") && !t.getText().equals(")")) {
				String token = ((TokenTerm) t).getText().trim();
				if (!token.startsWith("\"") || !token.endsWith("\""))
					return;

				String str = token.substring(1, token.length() - 1);
				Expression expr = new StringExpression(str);
				keywords.add(str);
				exprStack.add(expr);
			}
		}
	}

	private class StringExpression implements Expression {
		private String str;

		public StringExpression(String str) {
			this.str = str;
		}

		@Override
		public Object eval(Row map) {
			@SuppressWarnings("unchecked")
			Set<String> s = (Set<String>) map.get("result");
			return s.contains(str);
		}

		@Override
		public String toString() {
			return Strings.doubleQuote(str);
		}
	}

	private class AcSearchFuncEmitterFactory implements FuncEmitterFactory {
		@Override
		public void emit(QueryContext context, Stack<Expression> exprStack, FuncTerm f) {
		}
	}

	private enum AcSearchOpTerm implements OpTerm {
		And("and", 310), Or("or", 300), Not("not", 320), NOP("", 0);

		public String symbol;
		public int precedence;

		AcSearchOpTerm(String symbol, int precedence) {
			this.symbol = symbol;
			this.precedence = precedence;
		}

		@Override
		public String toString() {
			return symbol;
		}

		@Override
		public OpTerm parse(String token) {
			for (AcSearchOpTerm op : values()) {
				if (op.symbol.equals(token)) {
					return op;
				}
			}
			return null;
		}

		@Override
		public boolean isInstance(Object o) {
			return (o instanceof AcSearchOpTerm);
		}

		@Override
		public boolean isUnary() {
			return this.equals(Not);
		}

		@Override
		public boolean isAlpha() {
			return true;
		}

		@Override
		public boolean isLeftAssoc() {
			return !this.equals(Not);
		}

		@Override
		public boolean isDelimiter(String s) {
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
			return new ArrayList<OpTerm>();
		}

		@Override
		public OpTerm postProcessCloseParen() {
			return null;
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
}
