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

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.ThreadSafe;
import org.araqne.logdb.query.expr.Expression;

public class Eval extends QueryCommand implements ThreadSafe {
	private String field;
	private Expression expr;

	public Eval(String field, Expression expr) {
		this.field = field;
		this.expr = expr;
	}

	@Override
	public String getName() {
		return "eval";
	}

	public String getField() {
		return field;
	}

	public Expression getExpression() {
		return expr;
	}

	@Override
	public void onPush(Row m) {
		m.put(field, expr.eval(m));
		pushPipe(m);
	}

	@Override
	public void onPush(RowBatch rowBatch) {
		if (rowBatch.selectedInUse) {
			for (int i = 0; i < rowBatch.size; i++) {
				int p = rowBatch.selected[i];
				Row row = rowBatch.rows[p];
				row.put(field, expr.eval(row));
			}
		} else {
			for (int i = 0; i < rowBatch.size; i++) {
				Row row = rowBatch.rows[i];
				row.put(field, expr.eval(row));
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
		return "eval " + field + "=" + expr;
	}
}
