package org.araqne.logstorage;

import java.util.Collection;
import java.util.concurrent.locks.Lock;

public interface TableLock extends Lock {
	int getTableId();

	String getLockOwner();

	Collection<String> getPurposes();
}
