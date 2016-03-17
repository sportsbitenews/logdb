package org.araqne.logdb.cep.offheap.storage;

import java.io.Closeable;

/**
 * 
 *  map의 entry 수(=hash 테이블 크기) 가 Integer max 보다 큰 일이 없으니까 이건 필요없을듯...
 * 
 */

public interface HugeStorageArea<V> extends Closeable {
	V getValue(long index);

	void setValue(long index, V value);
	
	void remove(long index);
}

