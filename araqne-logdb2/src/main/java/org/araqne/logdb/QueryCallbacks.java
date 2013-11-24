package org.araqne.logdb;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public class QueryCallbacks {
	private Set<QueryResultCallback> resultCallbacks = new CopyOnWriteArraySet<QueryResultCallback>();
	private Set<QueryStatusCallback> statusCallbacks = new CopyOnWriteArraySet<QueryStatusCallback>();
	private Set<QueryTimelineCallback> timelineCallbacks = new CopyOnWriteArraySet<QueryTimelineCallback>();

	public Set<QueryStatusCallback> getStatusCallbacks() {
		return statusCallbacks;
	}

	public Set<QueryResultCallback> getResultCallbacks() {
		return resultCallbacks;
	}

	public Set<QueryTimelineCallback> getTimelineCallbacks() {
		return timelineCallbacks;
	}
}
