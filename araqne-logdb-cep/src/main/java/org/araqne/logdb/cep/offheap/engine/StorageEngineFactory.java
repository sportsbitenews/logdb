package org.araqne.logdb.cep.offheap.engine;

public interface StorageEngineFactory<K, V> {
	StorageEngine<K, V> instance();
}
