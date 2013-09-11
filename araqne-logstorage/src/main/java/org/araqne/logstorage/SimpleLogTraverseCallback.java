package org.araqne.logstorage;

import java.util.List;

public class SimpleLogTraverseCallback extends LogTraverseCallback {
	public SimpleLogTraverseCallback(Sink sink) {
		super(sink);
	}

	@Override
	public void interrupt() {
	}

	@Override
	public boolean isInterrupted() {
		return false;
	}

	@Override
	protected List<Log> filter(List<Log> logs) {
		return logs;
	}
}
