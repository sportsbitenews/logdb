package org.araqne.logdb.cep.offheap.factory;

import org.araqne.logdb.cep.EventContext;
import org.araqne.logdb.cep.EventKey;
import org.araqne.logdb.cep.offheap.OffheapTimeoutMap;
import org.araqne.logdb.cep.offheap.TimeoutMap;
import org.araqne.logdb.cep.offheap.engine.serialize.Serialize;
import org.araqne.logdb.cep.offheap.manager.EntryStorageManagerImpl;
import org.araqne.logdb.cep.offheap.manager.EventEntryStorageManager;

public class TimeoutMapFactory<K, V> {
	private StorageFactory<K, V> factory;

	public TimeoutMapFactory(StorageFactory<K, V> factory) {
		this.factory = factory;
	}

	public TimeoutMap<K, V> map() {
		return new OffheapTimeoutMap<K, V>(factory.instance());
	}

	public static TimeoutMapFactory<String, String> string(int indexSize, int bufferSize) {
		return new TimeoutMapFactory<String, String>(new ReferenceStorageFactory<String, String>(indexSize,
				new EntryStorageManagerImpl<String, String>(bufferSize, Serialize.STRING, Serialize.STRING)));
	}

	public static TimeoutMapFactory<EventKey, EventContext> event(int indexSize, int bufferSize) {
		return new TimeoutMapFactory<EventKey, EventContext>(new ReferenceStorageFactory<EventKey, EventContext>(
				indexSize, new EventEntryStorageManager(bufferSize)));
	}
}
