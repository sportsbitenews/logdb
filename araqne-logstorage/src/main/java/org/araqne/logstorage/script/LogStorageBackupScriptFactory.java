/*
 * Copyright 2013 Eediom Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import org.araqne.logstorage.dump.DumpService;

/**
 * @since 2.3.0
 * @author xeraph
 * 
 */
@Component(name = "logstorage-backup-script-factory")
@Provides
public class LogStorageBackupScriptFactory implements ScriptFactory {
	@ServiceProperty(name = "alias", value = "logstorage")
	private String alias;

	@Requires
	private LogTableRegistry tableRegistry;

	@Requires
	private StorageBackupManager backupManager;

	@Requires
	private StorageBackupMediaRegistry mediaRegistry;
	
	@Requires
	private DumpService dumpService;

	@Override
	public Script createScript() {
		return new LogStorageBackupScript(tableRegistry, backupManager, mediaRegistry, dumpService);
	}

}
