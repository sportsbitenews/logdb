package org.araqne.logdb.cep.offheap.factory;

import org.araqne.logdb.cep.offheap.engine.Storage;

public interface StorageFactory<K, V> {
	Storage<K, V> instance();
}
