package org.araqne.logdb.query.engine;

public interface QueryTaskListener {
	void onStart(QueryTaskEvent event);

	void onComplete(QueryTaskEvent event);
}
