package org.araqne.logstorage.backup;

import java.util.List;

public interface StorageBackupMediaRegistry {
	List<StorageBackupMediaFactory> getFactories();

	StorageBackupMediaFactory getFactory(String name);

	void registerFactory(StorageBackupMediaFactory factory);

	void unregisterFactory(StorageBackupMediaFactory factory);
}
