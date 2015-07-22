package org.araqne.logdb.query.parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.regex.Pattern;

import org.araqne.log.api.WildcardMatcher;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;
import org.araqne.logdb.query.command.StorageObjectName;
import org.araqne.logdb.query.expr.Comma;
import org.araqne.logdb.query.expr.Expression;
import org.araqne.logdb.query.parser.ExpressionParser.FuncTerm;
import org.araqne.logdb.query.parser.ExpressionParser.TokenTerm;
import org.araqne.logstorage.LogTableRegistry; 
import org.araqne.logstorage.TableSchema;

public class MetadataMatcher<T extends StorageObjectSpec> {
	public static final String IDFACTORY_KEY = "identifier_factory";
	private List<T> specs;
	private TableMatcher matcherExpr;

	public static interface IdentifierFactory {
		Object create(String concreteString);
	}

	public MetadataMatcher(String predicate, List<T> specs) {
		this.specs = specs;

		OpEmitterFactory of = new OpEmitter();
		FuncEmitterFactory ff = new FuncEmitter();
		TermEmitterFactory tf = new TermEmitter();

		Expression pred = ExpressionParser.parse(null, predicate, new ParsingRule(Ops.NOP, of, ff, tf));

		matcherExpr = ((TableMatcher) pred.eval(new Row()));
	}

	public List<StorageObjectName> match(LogTableRegistry tableRegistry) {
		ArrayList<StorageObjectName> result = new ArrayList<StorageObjectName>();

		for (T spec : specs) {
			List<StorageObjectName> match = spec.match(tableRegistry);
			for (StorageObjectName n : match) {
				if (matcherExpr.match(tableRegistry, n)) {
					result.add(n);
				}
			}
		}

		return result;
	}

	private static enum Ops implements OpTerm {
		Eq("==", 400), Neq("!=", 400), And("and", 310, true, false, true), Or("or", 300, true, false, true), Not("not", 320,
				false, true, true), Comma(",", 200), ListEndComma(",", 200), NOP("", 0, true, false, true);

		Ops(String symbol, int precedence) {
			this(symbol, precedence, true, false, false);
		}

		Ops(String symbol, int precedence, boolean leftAssoc, boolean unary, boolean isAlpha) {
			this.symbol = symbol;
			this.precedence = precedence;
			this.leftAssoc = leftAssoc;
			this.unary = unary;
			this.isAlpha = isAlpha;
		}

		public String symbol;
		public int precedence;
		public boolean leftAssoc;
		public boolean unary;
		public boolean isAlpha;

		@Override
		public String toString() {
			return symbol;
		}

		@Override
		public OpTerm parse(String token) {
			for (Ops op : values()) {
				if (op.symbol.equals(token) && op != NOP) {
					return op;
				}
			}
			return null;
		}

		@Override
		public boolean isInstance(Object o) {
			return o instanceof Ops;
		}

		@Override
		public boolean isUnary() {
			return unary && this.equals(Not);
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
			for (Ops op : values()) {
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
			for (Ops op : values()) {
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

	private static interface TableMatcher {
		boolean match(LogTableRegistry tr, StorageObjectName tableName);
	}

	private static abstract class BinaryExpression implements Expression {
		protected Expression lhs;
		protected Expression rhs;

		public BinaryExpression(Expression lhs, Expression rhs) {
			this.lhs = lhs;
			this.rhs = rhs;
		}

		public Object eval(Row map) {
			return eval(lhs.eval(map), rhs.eval(map));
		}

		public abstract TableMatcher eval(Object lhs, Object rhs);
	}

	private static class TableMetadataMatcher implements TableMatcher {
		private String key;
		private String value;
		private Pattern pattern = null;

		public TableMetadataMatcher(String key, String value) {
			this.key = key;
			this.value = value;
			this.pattern = WildcardMatcher.buildPattern(value);
		}

		@Override
		public boolean match(LogTableRegistry tr, StorageObjectName o) {
			TableSchema schema = tr.getTableSchema(o.getTable(), true);
			String rv = schema.getMetadata().get(key);
			if (pattern != null) {
				if (rv == null)
					return false;
				else
					return pattern.matcher(rv).matches();
			} else {
				if (value.equals(rv))
					return true;
				else
					return false;
			}
		}
	}

	private static class Eq extends BinaryExpression {
		public Eq(Expression lhs, Expression rhs) {
			super(lhs, rhs);
		}

		@Override
		public TableMatcher eval(Object lhs, Object rhs) {
			return new TableMetadataMatcher(lhs.toString(), rhs.toString());
		}
	}

	private static class BooleanMatcher implements TableMatcher {
		static enum Type {
			And, Or, Not
		}

		Type type;
		TableMatcher lhs;
		TableMatcher rhs;

		BooleanMatcher(Type type, TableMatcher lhs, TableMatcher rhs) {
			this.type = type;
			this.lhs = lhs;
			this.rhs = rhs;
		}

		BooleanMatcher(Type type, TableMatcher operand) {
			if (!Type.Not.equals(type))
				throw new IllegalArgumentException();
			this.type = Type.Not;
			this.lhs = operand;
		}

		@Override
		public boolean match(LogTableRegistry tr, StorageObjectName o) {
			switch (type) {
			case And:
				return lhs.match(tr, o) && rhs.match(tr, o);
			case Or:
				return lhs.match(tr, o) || rhs.match(tr, o);
			case Not:
				return !lhs.match(tr, o);
			default:
				throw new IllegalStateException(type.toString());
			}
		}
	}

	private static class And extends BinaryExpression {
		public And(Expression lhs, Expression rhs) {
			super(lhs, rhs);
		}

		@Override
		public TableMatcher eval(Object lhs, Object rhs) {
			return new BooleanMatcher(BooleanMatcher.Type.And, (TableMatcher) lhs, (TableMatcher) rhs);
		}

	}

	private static class Or extends BinaryExpression {
		public Or(Expression lhs, Expression rhs) {
			super(lhs, rhs);
		}

		@Override
		public TableMatcher eval(Object lhs, Object rhs) {
			return new BooleanMatcher(BooleanMatcher.Type.Or, (TableMatcher) lhs, (TableMatcher) rhs);
		}
	}

	private static class Not implements Expression {
		private Expression operand;

		public Not(Expression operand) {
			this.operand = operand;
		}

		@Override
		public Object eval(Row map) {
			return new BooleanMatcher(BooleanMatcher.Type.Not, (TableMatcher) operand.eval(map));
		}
	}

	private static class OpEmitter implements OpEmitterFactory {
		@Override
		public void emit(Stack<Expression> exprStack, Term term) {
			Ops op = (Ops) term;

			if (op.isUnary()) {
				Expression expr = exprStack.pop();
				switch (op) {
				case Not: {
					exprStack.add(new Not(expr));
					break;
				}

				default:
					throw new IllegalStateException("unimplemented unary operator " + op.toString());
				}
				return;
			}

			// reversed order by stack
			if (exprStack.size() < 2){
			//	throw new QueryParseException("broken-expression", -1, "operator is [" + op + "]");
				Map<String, String> params = new HashMap<String, String>();
				params.put("option", op.toString());
				throw new QueryParseException("90300", -1 , -1, params);
			}
			Expression rhs = exprStack.pop();
			Expression lhs = exprStack.pop();

			switch (op) {
			case And:
				exprStack.add(new And(lhs, rhs));
				break;
			case Or:
				exprStack.add(new Or(lhs, rhs));
				break;
			case Eq:
				exprStack.add(new Eq(lhs, rhs));
				break;
			case Neq:
				exprStack.add(new Not(new Eq(lhs, rhs)));
				break;
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

	}

	private static class TermExpr implements Expression {
		private String token;

		public TermExpr(String token) {
			this.token = token;
		}

		@Override
		public Object eval(Row map) {
			return token;
		}
	}

	private static class TermEmitter implements TermEmitterFactory {
		@Override
		public void emit(Stack<Expression> exprStack, TokenTerm t) {
			String token = ((TokenTerm) t).getText().trim();
			if (token.startsWith("\"") && token.endsWith("\"")) {
				exprStack.add(new TermExpr(token.substring(1, token.length() - 1)));
			} else {
				exprStack.add(new TermExpr(token));
			}
		}
	}

	private static class FuncEmitter implements FuncEmitterFactory {
		@Override
		public void emit(QueryContext context, Stack<Expression> exprStack, FuncTerm f) {
		}
	}

}
