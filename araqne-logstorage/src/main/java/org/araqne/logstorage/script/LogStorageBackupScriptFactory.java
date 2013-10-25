package org.araqne.logstorage.script;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.ServiceProperty;
import org.araqne.api.Script;
import org.araqne.api.ScriptFactory;
import org.araqne.logstorage.LogTableRegistry;
import org.araqne.logstorage.backup.StorageBackupManager;
import org.araqne.logstorage.backup.StorageBackupMediaRegistry;

@Component(name = "logstorage-backup-script-factory")
@Provides
public class LogStorageBackupScriptFactory implements ScriptFactory {
	@SuppressWarnings("unused")
	@ServiceProperty(name = "alias", value = "logstorage")
	private String alias;

	@Requires
	private LogTableRegistry tableRegistry;

	@Requires
	private StorageBackupManager backupManager;

	@Requires
	private StorageBackupMediaRegistry mediaRegistry;

	@Override
	public Script createScript() {
		return new LogStorageBackupScript(tableRegistry, backupManager, mediaRegistry);
	}

}
