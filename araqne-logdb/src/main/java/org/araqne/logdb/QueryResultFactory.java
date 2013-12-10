package org.araqne.logdb;

import java.io.IOException;

public interface QueryResultFactory {
	QueryResult createResult(QueryResultConfig config) throws IOException;

	void registerStorage(QueryResultStorage storage);

	void unregisterStorage(QueryResultStorage storage);
}
