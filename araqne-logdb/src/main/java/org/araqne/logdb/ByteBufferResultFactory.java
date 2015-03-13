package org.araqne.logdb;

import java.io.IOException;

import org.araqne.storage.api.RCDirectBufferManager;

public class ByteBufferResultFactory implements QueryResultFactory{
	private RCDirectBufferManager directBufferManager;
	int capacity;
	
	public ByteBufferResultFactory(RCDirectBufferManager directBufferManager, int capacity) {
		this.directBufferManager = directBufferManager;
		this.capacity = capacity;
	}

	@Override
	public QueryResult createResult(QueryResultConfig config) throws IOException {
		return new ByteBufferResult(config, this.directBufferManager, this.capacity);
	}

	@Override
	public void registerStorage(QueryResultStorage storage) {
	}

	@Override
	public void unregisterStorage(QueryResultStorage storage) {
	}

	@Override
	public void start() {
		
	}

}
