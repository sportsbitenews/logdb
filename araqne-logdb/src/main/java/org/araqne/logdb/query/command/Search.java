/*
 * Copyright 2011 Future Systems
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
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.query.expr.Expression;

public class Search extends QueryCommand {
	private long count;
	private final Long limit;
	private Expression expr;

	public Search(Long limit, Expression expr) {
		this.limit = limit;
		this.expr = expr;
	}

	public Long getLimit() {
		return limit;
	}

	public Expression getExpression() {
		return expr;
	}

	@Override
	public void onStart() {
		super.onStart();
	}

	@Override
	public void onPush(RowBatch rowBatch) {
		if (expr == null) {
			// always bypass
			if (limit == null) {
				pushPipe(rowBatch);
				count += rowBatch.size;
				return;
			}

			// bypass until reach the limit
			if (rowBatch.size + count <= limit) {
				pushPipe(rowBatch);
				count += rowBatch.size;
				return;
			}

			int more = (int) (limit - count);

			if (rowBatch.selectedInUse) {
				rowBatch.size = more;
			} else {
				rowBatch.selected = new int[more];
				rowBatch.selectedInUse = true;
				rowBatch.size = more;
				for (int i = 0; i < more; i++)
					rowBatch.selected[i] = i;
			}

			pushPipe(rowBatch);
			count += more;
			getQuery().stop(QueryStopReason.PartialFetch);
			return;
		}

		boolean ret;

		if (rowBatch.selectedInUse) {
			int n = 0;
			for (int i = 0; i < rowBatch.size; i++) {
				int p = rowBatch.selected[i];
				Row row = rowBatch.rows[p];

				Object o = expr.eval(row);
				if (o instanceof Boolean)
					ret = (Boolean) o;
				else
					ret = o != null;

				if (ret)
					rowBatch.selected[n++] = p;
			}

			rowBatch.size = n;
		} else {
			int n = 0;
			rowBatch.selected = new int[rowBatch.size];
			for (int i = 0; i < rowBatch.size; i++) {
				Row row = rowBatch.rows[i];
				Object o = expr.eval(row);
				if (o instanceof Boolean)
					ret = (Boolean) o;
				else
					ret = o != null;

				if (ret)
					rowBatch.selected[n++] = i;
			}

			rowBatch.size = n;
			rowBatch.selectedInUse = true;
		}

		// apply limit
		if (limit == null || rowBatch.size + count <= limit) {
			pushPipe(rowBatch);
			count += rowBatch.size;
			return;
		}

		int more = (int) (limit - count);
		rowBatch.size = more;
		pushPipe(rowBatch);
		count += more;
		getQuery().stop(QueryStopReason.PartialFetch);
	}

	@Override
	public void onPush(Row m) {
		boolean ret;

		if (expr != null) {
			Object o = expr.eval(m);
			if (o instanceof Boolean)
				ret = (Boolean) o;
			else
				ret = o != null;

			if (!ret)
				return;
		}

		pushPipe(m);

		if (limit != null && ++count >= limit)
			getQuery().stop(QueryStopReason.PartialFetch);
	}

	@Override
	public boolean isReducer() {
		return false;
	}

	@Override
	public String toString() {
		String limitOption = "";
		if (limit != null)
			limitOption = " limit=" + limit;
		return "search" + limitOption + (expr == null ? "" : " " + expr.toString());
	}
}
