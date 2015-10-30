package org.araqne.logstorage;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public interface TableLock {
	public static interface Purpose {
		String getName();
		UUID getUUID();
	}
	
	UUID lock();
	
	UUID lockInterruptibly() throws InterruptedException;
	
	UUID tryLock();
	
	UUID tryLock(long time, TimeUnit unit) throws InterruptedException;

	void unlock();
	
	int getTableId();

	String getLockOwner();

	Collection<Purpose> getPurposes();
}
	
