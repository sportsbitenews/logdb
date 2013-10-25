package org.araqne.logstorage.backup;

import java.util.List;
import java.util.Map;

public interface StorageBackupMediaFactory {
	String getName();

	List<StorageBackupConfigSpec> getConfigSpecs();

	StorageBackupMedia newMedia(Map<String, String> configs);
}
