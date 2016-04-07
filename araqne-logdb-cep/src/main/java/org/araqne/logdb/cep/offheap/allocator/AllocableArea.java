package org.araqne.logdb.cep.offheap.allocator;

import org.araqne.logdb.cep.offheap.storageArea.StorageArea;

public interface AllocableArea<V> extends StorageArea<V> {

	void setAddress(int index, int address);

	int getAddress(int index);

	int capacity();
}
