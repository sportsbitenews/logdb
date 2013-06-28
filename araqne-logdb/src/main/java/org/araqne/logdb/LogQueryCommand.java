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

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public abstract class LogQueryCommand {
	public static enum Status {
		Waiting, Running, End, Finalizing
	}

	private boolean canceled = false;
	private String queryString;
	private int pushCount;
	protected LogQuery logQuery;
	protected LogQueryCommand next;
	private boolean callbackTimeline;
	protected volatile Status status = Status.Waiting;
	protected Map<String, String> headerColumn = new HashMap<String, String>();

	public LogQueryCommand() {
		// default metadata column mappings
		headerColumn.put("table", "_table");
		headerColumn.put("id", "_id");
		headerColumn.put("date", "_time");
	}

	public String getQueryString() {
		return queryString;
	}

	public void setQueryString(String queryString) {
		this.queryString = queryString;
	}

	public void setLogQuery(LogQuery logQuery) {
		this.logQuery = logQuery;
	}

	public LogQueryCommand getNextCommand() {
		return next;
	}

	public void setNextCommand(LogQueryCommand next) {
		this.next = next;
	}

	public Status getStatus() {
		return status;
	}

	public void init() {
		this.status = Status.Waiting;
		if (next != null)
			next.headerColumn = this.headerColumn;
	}

	public void start() {
		throw new UnsupportedOperationException();
	}

	public int getPushCount() {
		return pushCount;
	}

	public abstract void push(LogMap m);

	protected final void write(LogMap m) {
		pushCount++;
		if (next != null && next.status != Status.End) {
			if (callbackTimeline) {
				for (LogTimelineCallback callback : logQuery.getTimelineCallbacks())
					callback.put((Date) m.get(headerColumn.get("date")));
			}
			next.status = Status.Running;
			next.push(m);
		}
	}

	public abstract boolean isReducer();

	public boolean isCallbackTimeline() {
		return callbackTimeline;
	}

	public void setCallbackTimeline(boolean callbackTimeline) {
		this.callbackTimeline = callbackTimeline;
	}

	public void eof(boolean canceled) {
		status = Status.End;
		
		this.canceled = canceled;

		if (next != null && next.status != Status.End && next.status != Status.Finalizing)
			next.eof(canceled);

		if (logQuery != null) {
			if (callbackTimeline) {
				for (LogTimelineCallback callback : logQuery.getTimelineCallbacks())
					callback.eof(canceled);
				logQuery.clearTimelineCallbacks();
			}
			if (logQuery.getCommands().get(0).status != Status.End)
				logQuery.getCommands().get(0).eof(canceled);
		}
	}
}
