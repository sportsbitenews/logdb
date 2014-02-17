/*
 * Copyright 2013 Eediom Inc.
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

import java.util.Map;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.ThreadSafe;
import org.araqne.logdb.query.expr.Expression;

public class Evalc extends QueryCommand implements ThreadSafe {
	private final Map<String, Object> constants;
	private final String constantName;
	private final Expression expr;

	public Evalc(QueryContext context, String constantName, Expression expr) {
		this.constants = context.getConstants();
		this.constantName = constantName;
		this.expr = expr;
	}

	@Override
	public String getName() {
		return "evalc";
	}

	public String getConstantName() {
		return constantName;
	}

	public Expression getExpression() {
		return expr;
	}

	@Override
	public void onPush(Row m) {
		constants.put(constantName, expr.eval(m));
		pushPipe(m);
	}

	@Override
	public void onPush(RowBatch rowBatch) {
		// expr may evaluate query context variable
		// do not optimize such as 'put only last value'

		if (rowBatch.selectedInUse) {
			for (int i = 0; i < rowBatch.size; i++) {
				int p = rowBatch.selected[i];
				Row row = rowBatch.rows[p];
				constants.put(constantName, expr.eval(row));
			}
		} else {
			for (Row row : rowBatch.rows)
				constants.put(constantName, expr.eval(row));
		}

		pushPipe(rowBatch);
	}

	@Override
	public String toString() {
		return "evalc " + constantName + "=" + expr;
	}

}
