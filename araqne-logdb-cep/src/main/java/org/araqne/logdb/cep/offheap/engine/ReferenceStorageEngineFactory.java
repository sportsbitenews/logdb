package org.araqne.logdb.cep.offheap.engine;

import org.araqne.logdb.cep.offheap.engine.serialize.Serialize;

public class ReferenceStorageEngineFactory<K, V> implements StorageEngineFactory<K, V> {

	private Serialize<K> keySerializer;
	private Serialize<V> valueSerializer;
	private int indexSize = 1024;
	private int storageSize = 2047;

	public ReferenceStorageEngineFactory(Serialize<K> keySerializer, Serialize<V> valueSerializer) {
		this(1024, 2047, keySerializer, valueSerializer);
	}

	public ReferenceStorageEngineFactory(int size, Serialize<K> keySerializer, Serialize<V> valueSerializer) {
		this(size, 2047, keySerializer, valueSerializer);
	}

	public ReferenceStorageEngineFactory(int idnexSize, int storageSize, Serialize<K> keySerializer,
			Serialize<V> valueSerializer) {
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.indexSize = idnexSize;
		this.storageSize = storageSize;
	}

	@Override
	public StorageEngine<K, V> instance() {
		return new ReferenceStorageEngine<K, V>(indexSize, storageSize, keySerializer, valueSerializer);
	}

}
