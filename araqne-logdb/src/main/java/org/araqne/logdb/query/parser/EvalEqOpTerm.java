/*
 * Copyright 2015 Eediom Inc.
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
import java.util.List;

public enum EvalEqOpTerm implements OpTerm {
	Add("+", 500), Sub("-", 500), Mul("*", 510), Div("/", 510), Neg("-", 520, false, true, false, Sub),
	Gte(">=", 410), Lte("<=", 410), Gt(">", 410), Lt("<", 410), Eq("==", 400), Neq("!=", 400),
	And("and", 310, true, false, true, null), Or("or", 300, true, false, true, null), 
	Assign("=", 250, false, false, false, null),
	Comma(",", 200), ListEndComma(",", 200),
	NOP("", 0, true, false, true, null)
	;
	
	EvalEqOpTerm(String symbol, int precedence) {
		this(symbol, precedence, true, false, false, null);
	}

	EvalEqOpTerm(String symbol, int precedence, boolean leftAssoc, boolean unary, boolean isAlpha, EvalEqOpTerm altOp) {
		this.symbol = symbol;
		this.precedence = precedence;
		this.leftAssoc = leftAssoc;
		this.unary = unary;
		this.isAlpha = isAlpha;
		this.altOp = altOp;
		if (altOp != null)
			altOp.altOp = this;
	}

	public final String symbol;
	public final int precedence;
	public final boolean leftAssoc;
	public final boolean unary;
	private EvalEqOpTerm altOp;
	public final boolean isAlpha;
	
	@Override
	public String toString() {
		return symbol;
	}

	@Override
	public OpTerm parse(String token) {
		for (EvalEqOpTerm op : values()) {
			if (op.symbol.equals(token) && op != NOP) {
				return op;
			}
		}
		return null;
	}

	@Override
	public boolean isInstance(Object o) {
		return o instanceof EvalEqOpTerm;
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
		for (EvalEqOpTerm op : values()) {
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
		for (EvalEqOpTerm op : values()) {
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
		return altOp != null;
	}

	@Override
	public OpTerm getAltOp() {
		return altOp;
	}
}
