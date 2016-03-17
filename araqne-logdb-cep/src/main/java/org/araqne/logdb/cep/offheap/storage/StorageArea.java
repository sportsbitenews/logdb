package org.araqne.logdb.cep.offheap.storage;

import java.io.Closeable;

public interface StorageArea<V> extends Closeable{
	V getValue(int index);

	void setValue(int index, V value);
	
	void remove(int index);
}

