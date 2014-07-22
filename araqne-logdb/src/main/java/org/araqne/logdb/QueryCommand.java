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
package org.araqne.logdb;

import java.util.ArrayList;
import java.util.List;

public abstract class QueryCommand {
	public static enum Status {
		Waiting, Running, End, Finalizing
	}

	protected Query query;
	private boolean cancelled = false;

	protected RowPipe output;
	private long outputCount;
	protected volatile Status status = Status.Waiting;

	public QueryTask getMainTask() {
		return null;
	}

	public long getOutputCount() {
		return outputCount;
	}

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	public RowPipe getOutput() {
		return output;
	}

	public void setOutput(RowPipe output) {
		this.output = output;
	}

	@Deprecated
	public boolean isCancelled() {
		return cancelled;
	}

	public abstract String getName();

	public QueryContext getContext() {
		return query.getContext();
	}

	public Query getQuery() {
		return query;
	}

	public void setQuery(Query query) {
		this.query = query;
	}

	public void onStart() {
		// override this for initialization
	}

	public void onClose(QueryStopReason reason) {
		// override this for resource clean up
	}

	public void onPush(Row row) {
		pushPipe(row);
	}

	// default adapter for old per-row processing
	public void onPush(RowBatch rowBatch) {
		if (rowBatch.selectedInUse) {
			for (int i = 0; i < rowBatch.size; i++) {
				Row row = rowBatch.rows[rowBatch.selected[i]];
				onPush(row);
			}
		} else {
			for (Row row : rowBatch.rows)
				onPush(row);
		}
	}

	protected final void pushPipe(Row row) {
		outputCount++;

		if (output != null) {
			if (output.isThreadSafe()) {
				output.onRow(row);
			} else {
				synchronized (output) {
					output.onRow(row);
				}
			}
		}
	}

	protected final void pushPipe(RowBatch rowBatch) {
		outputCount += rowBatch.size;
		if (output != null) {
			if (output.isThreadSafe()) {
				output.onRowBatch(rowBatch);
			} else {
				synchronized (output) {
					output.onRowBatch(rowBatch);
				}
			}
		}
	}

	public boolean isDriver() {
		return false;
	}

	public boolean isReducer() {
		return false;
	}

	public List<QueryCommand> getNestedCommands() {
		return new ArrayList<QueryCommand>();
	}
}
