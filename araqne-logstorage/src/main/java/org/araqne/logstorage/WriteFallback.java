package org.araqne.logstorage;

import java.util.List;

public interface WriteFallback {
	int onLockFailure(TableLock lock, List<Log> logs);
}
