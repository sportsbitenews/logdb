package org.araqne.logdb.cep.offheap.allocator;

import org.araqne.logdb.cep.offheap.storage.StorageArea;

//XXX short보다 작은 storage area를 위한 small allocator area?
public interface AllocableArea<V> extends StorageArea<V>{

	void setAddress(int index, int address);

	int getAddress(int index);
	
	int capacity();
}
