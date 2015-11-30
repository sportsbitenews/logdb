package org.araqne.logdb;

import java.io.IOException;

public class BypassResultFactory implements QueryResultFactory {
	private QueryCommand cmd;

	public BypassResultFactory(QueryCommand cmd) {
		this.cmd = cmd;
	}

	@Override
	public QueryResult createResult(QueryResultConfig config) throws IOException {
		return new BypassResult(cmd);
	}

	@Override
	public void registerStorage(QueryResultStorage storage) {
	}

	@Override
	public void unregisterStorage(QueryResultStorage storage) {
	}

	@Override
	public void start() {
		// TODO Auto-generated method stub

	}
}