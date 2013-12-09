package org.araqne.logdb.query.engine;

public class QueryTaskEvent {
	private QueryTask task;
	private boolean handled;

	public QueryTaskEvent(QueryTask task) {
		this.task = task;
	}

	public QueryTask getTask() {
		return task;
	}

	public boolean isHandled() {
		return handled;
	}

	public void setHandled(boolean handled) {
		this.handled = handled;
	}
}
