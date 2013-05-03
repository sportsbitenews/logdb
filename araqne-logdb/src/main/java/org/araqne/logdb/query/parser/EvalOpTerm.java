package org.araqne.logdb.query.parser;

import java.util.ArrayList;
import java.util.List;

import org.araqne.logdb.query.parser.OpTerm;

public enum EvalOpTerm implements OpTerm {
		Add("+", 500), Sub("-", 500), Mul("*", 510), Div("/", 510), Neg("-", 520, false, true, false),
		Gte(">=", 410), Lte("<=", 410), Gt(">", 410), Lt("<", 410), Eq("==", 400), Neq("!=", 400),
		And("and", 310, true, false, true), Or("or", 300, true, false, true), Not("not", 320, false, true, true),
		Comma(",", 200), ListEndComma(",", 200),
		From("from", 100, true, false, true), union("union", 110, false, true, true),
		NOP("", 0, true, false, true)
		;
		
		EvalOpTerm(String symbol, int precedence) {
			this(symbol, precedence, true, false, false);
		}

		EvalOpTerm(String symbol, int precedence, boolean leftAssoc, boolean unary, boolean isAlpha) {
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
			for (EvalOpTerm t : values())
				if (t.symbol.equals(token))
					return t;

			return null;
		}

		@Override
		public boolean isInstance(Object o) {
			return o instanceof EvalOpTerm;
		}
		
		@Override
		public boolean isUnary() {
			return unary && this.equals(Neg);
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
			for (EvalOpTerm op : values()) {
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
			for (EvalOpTerm op : values()) {
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
