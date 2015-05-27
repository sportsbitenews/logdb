package org.araqne.logdb.query.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.DefaultQuery;
import org.araqne.logdb.Query;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.QueryResultFactory;
import org.araqne.logdb.Row;
import org.araqne.logdb.query.command.Join;
import org.araqne.logdb.query.command.Join.JoinType;
import org.araqne.logdb.query.command.Sort.SortField;
import org.araqne.logdb.query.expr.Comma;
import org.araqne.logdb.query.expr.Expression;
import org.araqne.logdb.query.parser.ExpressionParser.FuncTerm;
import org.araqne.logdb.query.parser.ExpressionParser.TokenTerm;

public class JoinParser extends AbstractQueryCommandParser {

	private QueryParserService parserService;
	private QueryResultFactory resultFactory;

	public JoinParser(QueryParserService parserService, QueryResultFactory resultFactory) {
		this.parserService = parserService;
		this.resultFactory = resultFactory;
	}

	@Override
	public String getCommandName() {
		return "join";
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(), Arrays.asList("type"), getFunctionRegistry());
		@SuppressWarnings("unchecked")
		Map<String, Object> options = (Map<String, Object>) r.value;

		String type = null;
		if (options != null) {
			type = (String) options.get("type");
		}

		if (r.next < 0)
			r.next = 0;

		JoinType joinType = JoinType.Inner;
		if (type != null && type.equals("left"))
			joinType = JoinType.Left;
		
		// parsing rule for join
		OpEmitterFactory of = new JoinOpEmitterFactory();
		TermEmitterFactory tf = new JoinTermEmitterFactory();
		FuncEmitterFactory ff = new FuncEmitterFactory() {
			@Override
			public void emit(QueryContext context, Stack<Expression> exprStack, FuncTerm f) {
				throw new QueryParseException("unexpected-function", -1, "function is [" + f + "]");
			}
		};
		
		int b = commandString.indexOf('[', r.next);
		if (b < 0)
			throw new QueryParseException("no-subquery", -1, "join query has no subquery");
		
		String fieldString = commandString.substring(r.next, b);
		
		Expression fExpr = ExpressionParser.parse(context, fieldString, new ParsingRule(JoinOpTerm.NOP, of, ff, tf));
		SortField[] sortFieldArray = null;
		if (fExpr instanceof FieldTerm)
			sortFieldArray = new SortField[] { SortField.class.cast(fExpr.eval(null)) };
		else if (fExpr instanceof Comma) {
			@SuppressWarnings("unchecked")
			List<Object> fl = (List<Object>)fExpr.eval(null);
			sortFieldArray = fl.toArray(new SortField[0]);
		} else {
			throw new QueryParseException("unexpected-expression", -1, "expression is [" + fieldString + "]");
		}
		
		Expression sqExpr = ExpressionParser.parse(context, commandString.substring(b), new ParsingRule(JoinOpTerm.NOP, of, ff, tf));
		if (!(sqExpr instanceof SubQueryTerm))
			throw new QueryParseException("no-subquery", -1, "join query has no subquery");
		
		String subQueryString = SubQueryTerm.class.cast(sqExpr).getSubQuery();
		List<QueryCommand> subCommands = parserService.parseCommands(context, subQueryString);
		Query subQuery = new DefaultQuery(context, subQueryString, subCommands, resultFactory);
		return new Join(joinType, sortFieldArray, subQuery);
	}
	
	private static enum JoinOpTerm implements OpTerm {
		Asc("+", 500, false, true, false), Desc("-", 500, false, true, false),
		Comma(",", 200), ListEndComma(",", 200),
		NOP("", 0, true, false, true)
		;
		
		private JoinOpTerm(String symbol, int precedence) {
			this(symbol, precedence, true, false, false);
		}
		
		private JoinOpTerm(String symbol, int precedence, boolean leftAssoc, boolean unary, boolean isAlpha) {
			this.symbol = symbol;
			this.precedence = precedence;
			this.leftAssoc = leftAssoc;
			this.unary = unary;
			this.isAlpha = isAlpha;
		}

		public final String symbol;
		public final int precedence;
		public final boolean leftAssoc;
		public final boolean unary;
		public final boolean isAlpha;
		
		@Override
		public String toString() {
			return symbol;
		}

		@Override
		public boolean isInstance(Object o) {
			return o instanceof JoinOpTerm;
		}

		@Override
		public OpTerm parse(String token) {
			for (JoinOpTerm op : values()) {
				if (op.symbol.equals(token) && op != NOP) {
					return op;
				}
			}
			return null;
		}

		@Override
		public boolean isDelimiter(String s) {
			for (JoinOpTerm op : values()) {
				if (!op.isAlpha && op.symbol.equals(s)) {
					return true;
				}
			}
			return false;
		}

		@Override
		public List<OpTerm> delimiters() {
			List<OpTerm> delims = new ArrayList<OpTerm>();
			for (JoinOpTerm op : values()) {
				if (!op.isAlpha) {
					delims.add(op);
				}
			}
			return delims;
		}

		@Override
		public boolean isUnary() {
			return unary;
		}

		@Override
		public boolean isAlpha() {
			return isAlpha;
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
		public boolean isLeftAssoc() {
			return leftAssoc;
		}

		@Override
		public OpTerm postProcessCloseParen() {
			if (!this.equals(Comma)) {
				return this;
			}
			
			return ListEndComma;
		}
		
	}
	
	private static class JoinOpEmitterFactory implements OpEmitterFactory {

		@Override
		public void emit(Stack<Expression> exprStack, Term term) {
			JoinOpTerm op = JoinOpTerm.class.cast(term);
			
			// is unary op?
			if (op.isUnary()) {
				Expression expr = exprStack.pop();
				if (!(expr instanceof FieldTerm))
					throw new QueryParseException("unexpected-expression", -1, "expression is [" + expr + "]");
				
				FieldTerm t = FieldTerm.class.cast(expr);
				switch (op) {
				case Asc: {
					exprStack.push(t);
					break;
				}
				case Desc: {
					t.toggleAsc();
					exprStack.push(t);
					break;
				}
				default:
					throw new QueryParseException("unsupported-operator", -1, "unsupported unary operator [" + op.toString() + "]");
				}
				return;
			}
			
			// reversed order by stack
			if (exprStack.size() < 2)
				throw new QueryParseException("broken-expression", -1, "operator is [" + op + "]");

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
				throw new QueryParseException("unsupported-operator", -1, "unsupported unary operator [" + op.toString() + "]");
			}
		}
		
	}
	
	private static class SubQueryTerm implements Expression {
		private final String subquery;
		
		public SubQueryTerm(String subquery) {
			this.subquery = subquery;
		}
		
		@Override
		public Object eval(Row row) {
			return subquery;
		}
		
		public String getSubQuery() {
			return subquery;
		}
	}

	private static class FieldTerm implements Expression {
		private final String field;
		private boolean asc;
		
		public FieldTerm(String field) {
			this.field = field;
			this.asc = true;
		}
		
		public boolean toggleAsc() {
			asc = !asc;
			return asc;
		}
		
		@Override
		public Object eval(Row row) {
			return new SortField(field, asc);
		}
		
	}
	
	private static class JoinTermEmitterFactory implements TermEmitterFactory {
		@Override
		public void emit(Stack<Expression> exprStack, TokenTerm t) {
			if (!t.getText().equals("(") && !t.getText().equals(")")) {
				String token = ((TokenTerm) t).getText().trim();
				Expression expr = parseTokenExpr(exprStack, token);
				exprStack.add(expr);
			}
		}
		
		private Expression parseTokenExpr(Stack<Expression> exprStack, String token) {
			// sub query
			if (token.startsWith("[") && token.endsWith("]")) {
				String subQueryString = token.substring(1, token.length() - 1).trim();
				SubQueryTerm term = new SubQueryTerm(subQueryString);
				return term;
			}
			
			return new FieldTerm(token);
		}
	}
}
