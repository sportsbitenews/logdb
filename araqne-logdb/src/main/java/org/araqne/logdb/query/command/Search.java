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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.ThreadSafe;
import org.araqne.logdb.VectorizedRowBatch;
import org.araqne.logdb.query.expr.Expression;
import org.araqne.logdb.query.expr.VectorizedExpression;

public class Search extends QueryCommand implements ThreadSafe {
	private AtomicLong count = new AtomicLong();
	private final Long limit;
	private final Expression expr;
	private final boolean vectorized;

	// for accurate limit
	private ReentrantLock lock = new ReentrantLock();

	public Search(Long limit, Expression expr) {
		this.limit = limit;
		this.expr = expr;
		this.vectorized = expr instanceof VectorizedExpression;
	}

	@Override
	public String getName() {
		return "search";
	}

	public Long getLimit() {
		return limit;
	}

	public Expression getExpression() {
		return expr;
	}

	@Override
	public void onPush(VectorizedRowBatch vbatch) {
		if (expr == null) {
			// always bypass
			if (limit == null) {
				pushPipe(vbatch);
				count.addAndGet(vbatch.size);
				return;
			}

			lock.lock();
			try {
				// bypass until reach the limit
				if (vbatch.size + count.get() <= limit) {
					pushPipe(vbatch);
					count.addAndGet(vbatch.size);
					return;
				}

				int more = (int) (limit - count.get());

				if (vbatch.selectedInUse) {
					vbatch.size = more;
				} else {
					vbatch.selected = new int[more];
					vbatch.selectedInUse = true;
					vbatch.size = more;
					for (int i = 0; i < more; i++)
						vbatch.selected[i] = i;
				}

				pushPipe(vbatch);
				count.addAndGet(more);
				getQuery().cancel(QueryStopReason.PartialFetch);
			} finally {
				lock.unlock();
			}
			return;
		}

		Object[] values = null;
		boolean[] retArray = null;
		if (vectorized) {
			values = ((VectorizedExpression) expr).eval(vbatch);
			retArray = new boolean[vbatch.size];
			for (int i = 0; i < vbatch.size; i++) {
				Object o = values[i];
				if (o instanceof Boolean)
					retArray[i] = (Boolean) o;
				else
					retArray[i] = o != null;
			}

			int n = 0;
			if (vbatch.selectedInUse) {
				for (int i = 0; i < vbatch.size; i++) {
					int p = vbatch.selected[i];
					if (retArray[i])
						vbatch.selected[n++] = p;
				}
			} else {
				vbatch.selectedInUse = true;
				vbatch.selected = new int[vbatch.size];
				for (int i = 0; i < vbatch.size; i++) {
					if (retArray[i])
						vbatch.selected[n++] = i;
				}
			}
			vbatch.size = n;
		} else {
			// TODO: vectorize all expressions and remove this block.
			boolean ret;
			if (vbatch.selectedInUse) {
				int n = 0;
				for (int i = 0; i < vbatch.size; i++) {
					int p = vbatch.selected[i];
					Row row = vbatch.row(p);

					Object o = expr.eval(row);
					if (o instanceof Boolean)
						ret = (Boolean) o;
					else
						ret = o != null;

					if (ret)
						vbatch.selected[n++] = p;
				}

				vbatch.size = n;
			} else {
				int n = 0;
				vbatch.selected = new int[vbatch.size];
				for (int i = 0; i < vbatch.size; i++) {
					Row row = vbatch.row(i);
					Object o = expr.eval(row);
					if (o instanceof Boolean)
						ret = (Boolean) o;
					else
						ret = o != null;

					if (ret)
						vbatch.selected[n++] = i;
				}

				vbatch.size = n;
				vbatch.selectedInUse = true;
			}
		}
		
		if (vbatch.size == 0)
			return;

		// apply limit
		if (limit == null) {
			pushPipe(vbatch);
			count.addAndGet(vbatch.size);
			return;
		}

		lock.lock();
		try {
			if (vbatch.size + count.get() <= limit) {
				pushPipe(vbatch);
				count.addAndGet(vbatch.size);
				return;
			}

			int more = (int) (limit - count.get());
			if (more > 0) {
				vbatch.size = more;
				pushPipe(vbatch);
				count.addAndGet(more);
			}
			
			getQuery().cancel(QueryStopReason.PartialFetch);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void onPush(RowBatch rowBatch) {
		if (expr == null) {
			// always bypass
			if (limit == null) {
				pushPipe(rowBatch);
				count.addAndGet(rowBatch.size);
				return;
			}

			lock.lock();
			try {
				// bypass until reach the limit
				if (rowBatch.size + count.get() <= limit) {
					pushPipe(rowBatch);
					count.addAndGet(rowBatch.size);
					return;
				}

				int more = (int) (limit - count.get());

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
				count.addAndGet(more);
				getQuery().cancel(QueryStopReason.PartialFetch);
			} finally {
				lock.unlock();
			}
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
		if (limit == null) {
			pushPipe(rowBatch);
			count.addAndGet(rowBatch.size);
			return;
		}

		lock.lock();
		try {
			if (rowBatch.size + count.get() <= limit) {
				pushPipe(rowBatch);
				count.addAndGet(rowBatch.size);
				return;
			}

			int more = (int) (limit - count.get());
			rowBatch.size = more;
			pushPipe(rowBatch);
			count.addAndGet(more);
			getQuery().cancel(QueryStopReason.PartialFetch);
		} finally {
			lock.unlock();
		}
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

		lock.lock();
		try {
			pushPipe(m);

			if (limit != null && count.incrementAndGet() >= limit)
				getQuery().cancel(QueryStopReason.PartialFetch);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public String toString() {
		String limitOption = "";
		if (limit != null)
			limitOption = " limit=" + limit;
		return "search" + limitOption + (expr == null ? "" : " " + expr.toString());
	}
}
