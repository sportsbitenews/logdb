package org.araqne.logstorage;

import java.util.concurrent.locks.Lock;

public interface TableLock extends Lock {
	int getTableId();
}
