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
import java.util.Date;

public abstract class QueryCommand {
	public static enum Status {
		Waiting, Running, End, Finalizing
	}

	protected Query query;
	private boolean cancelled = false;

	// command name
	private String name;
	private String queryString;
	protected RowPipe output;
	private boolean invokeTimelineCallback;

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

	public boolean isCancelled() {
		return cancelled;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getQueryString() {
		return queryString;
	}

	public void setQueryString(String queryString) {
		this.queryString = queryString;
	}

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
			if (invokeTimelineCallback && query != null) {
				for (QueryTimelineCallback callback : query.getCallbacks().getTimelineCallbacks()) {
					Date date = row.getDate();
					if (date != null)
						callback.put((Date) date);
				}
			}

			output.onRow(row);
		}
	}

	protected final void pushPipe(RowBatch rowBatch) {
		outputCount += rowBatch.size;
		if (output != null) {
			if (invokeTimelineCallback && query != null) {
				for (QueryTimelineCallback callback : query.getCallbacks().getTimelineCallbacks()) {
					if (rowBatch.selectedInUse) {
						ArrayList<Date> dates = new ArrayList<Date>(rowBatch.size);
						for (int i = 0; i < rowBatch.size; i++) {
							Row row = rowBatch.rows[rowBatch.selected[i]];
							Date date = row.getDate();
							if (date != null)
								dates.add(date);
						}

						callback.put(dates);
					} else {
						ArrayList<Date> dates = new ArrayList<Date>(rowBatch.size);
						for (Row row : rowBatch.rows) {
							Date date = row.getDate();
							if (date != null)
								dates.add(date);
						}

						callback.put(dates);
					}
				}
			}

			output.onRowBatch(rowBatch);
		}
	}

	public boolean isDriver() {
		return false;
	}

	public boolean isReducer() {
		return false;
	}

	public boolean isInvokeTimelineCallback() {
		return invokeTimelineCallback;
	}

	public void setInvokeTimelineCallback(boolean invokeTimelineCallback) {
		this.invokeTimelineCallback = invokeTimelineCallback;
	}
}
