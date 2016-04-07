package org.araqne.logdb.cep.offheap.allocator;

public interface Allocator {
	int allocate(int size);

	void free(int address);

	void clear();

	int space(int address);
}
