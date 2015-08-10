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
package org.araqne.logdb.query.command;

import java.util.ArrayList;
import java.util.List;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.ThreadSafe;
import org.araqne.logdb.query.expr.Assign;
import org.araqne.logdb.query.expr.Comma;
import org.araqne.logdb.query.expr.Expression;

public class Eval extends QueryCommand implements ThreadSafe {
	private static final String COMMAND = "eval";

	private List<Expression> exprs;

	public Eval(Expression expr, int length) {
		if (expr instanceof Comma) {
			Comma ce = Comma.class.cast(expr);
			this.exprs = ce.getList();
		} else {
			this.exprs = new ArrayList<Expression>();
			exprs.add(expr);
		}
		
		for (Expression exp : exprs) {
			if (!(exp instanceof Assign)) {
				throw new QueryParseException("20100", COMMAND.length() + 1, length - 1, null);
			}
		}
	}
	
	public Expression getExpression(int idx) {
		return exprs.get(idx);
	}
	
	public List<Expression> getExpressions() {
		return exprs;
	}

	@Override
	public String getName() {
		return COMMAND;
	}

	public Object update(Row m, Expression expr) {
		if (!(expr instanceof Assign))
			return expr.eval(m);
		
		Assign ae = Assign.class.cast(expr);
		Object ret = update(m, ae.getValueExpression());
		m.put(ae.getField(), ret);
		return ret;
	}
	
	@Override
	public void onPush(Row m) {
		for (Expression expr : exprs) {
			update(m, expr);
		}
		pushPipe(m);
	}

	@Override
	public void onPush(RowBatch rowBatch) {
		if (rowBatch.selectedInUse) {
			for (int i = 0; i < rowBatch.size; i++) {
				int p = rowBatch.selected[i];
				Row row = rowBatch.rows[p];
				for (Expression expr : exprs) {
					update(row, expr);
				}
			}
		} else {
			for (int i = 0; i < rowBatch.size; i++) {
				Row row = rowBatch.rows[i];
				for (Expression expr : exprs) {
					update(row, expr);
				}
			}
		}

		pushPipe(rowBatch);
	}

	@Override
	public boolean isReducer() {
		return false;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("eval ");
		boolean first = true;
		for (Expression expr : exprs) {
			if (!first)
				sb.append(", ");
			sb.append(expr);
			if (first)
				first = false;
		}
		return sb.toString();
	}
}
