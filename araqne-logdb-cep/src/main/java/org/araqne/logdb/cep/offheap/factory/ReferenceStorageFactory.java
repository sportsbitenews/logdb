package org.araqne.logdb.cep.offheap.factory;

import org.araqne.logdb.cep.offheap.engine.ReferenceStorage;
import org.araqne.logdb.cep.offheap.engine.Storage;
import org.araqne.logdb.cep.offheap.manager.EntryStorageManager;

public class ReferenceStorageFactory<K, V> implements StorageFactory<K, V> {
	private EntryStorageManager<K, V> manager;
	private int indexSize = 0;

	public ReferenceStorageFactory(int idnexSize, EntryStorageManager<K, V> manager) {
		this.indexSize = idnexSize;
		this.manager = manager;
	}

	public Storage<K, V> instance() {
		return new ReferenceStorage<K, V>(indexSize, manager);
	}
}
