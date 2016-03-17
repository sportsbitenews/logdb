package org.araqne.logdb.cep.offheap.timeout;

import java.util.List;

public interface Expirable<K> {
	void setTime(long now, boolean force);

	void addListner();

	long getLastTime();

	void timeout(K key, long timeoutTsime);
	
	List<K> timeoutQueue(); 
	
	List<K> expireQueue(); 
}
