package org.araqne.logdb.cep.offheap.factory;

import org.araqne.logdb.cep.EventContext;
import org.araqne.logdb.cep.EventKey;
import org.araqne.logdb.cep.offheap.ConcurrentTimeoutMap;

public class ConcurrentTimeoutMapFactory<K, V> {
	private int concurrency;
	private TimeoutMapFactory<K, V> factory;

	public ConcurrentTimeoutMapFactory(TimeoutMapFactory<K, V> factory) {
		this(-1, factory);
	}

	public ConcurrentTimeoutMapFactory(int concurrency, TimeoutMapFactory<K, V> factory) {
		this.concurrency = concurrency;
		this.factory = factory;
	}

	public ConcurrentTimeoutMap<K, V> map() {
		if (concurrency > 0)
			return new ConcurrentTimeoutMap<K, V>(concurrency, factory);
		return new ConcurrentTimeoutMap<K, V>(factory);
	}

	public static ConcurrentTimeoutMapFactory<String, String> string(int indexSize, int bufferSize) {
		return new ConcurrentTimeoutMapFactory<String, String>(TimeoutMapFactory.string(indexSize, bufferSize));
	}

	public static ConcurrentTimeoutMapFactory<String, String> string(int con, int indexSize, int bufferSize) {
		return new ConcurrentTimeoutMapFactory<String, String>(con, TimeoutMapFactory.string(indexSize, bufferSize));
	}

	public static ConcurrentTimeoutMapFactory<EventKey, EventContext> event(int indexSize, int bufferSize) {
		return new ConcurrentTimeoutMapFactory<EventKey, EventContext>(TimeoutMapFactory.event(indexSize, bufferSize));
	}

	public static ConcurrentTimeoutMapFactory<EventKey, EventContext> event(int con, int indexSize, int bufferSize) {
		return new ConcurrentTimeoutMapFactory<EventKey, EventContext>(con, TimeoutMapFactory.event(indexSize,
				bufferSize));
	}
}