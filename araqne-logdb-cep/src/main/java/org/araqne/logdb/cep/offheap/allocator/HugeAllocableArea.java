package org.araqne.logdb.cep.offheap.allocator;

import org.araqne.logdb.cep.offheap.storage.StorageArea;

public interface HugeAllocableArea<V> extends StorageArea<V>{

	void setAddress(long index, long address);

	long getAddress(long index);
	
	long capacity();
	
}
