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
package org.araqne.logstorage.backup;

import java.io.IOException;
import java.util.List;

/**
 * @since 2.2.7
 * @author xeraph
 * 
 */
public interface StorageBackupManager {
	List<StorageBackupJob> getBackupJobs();

	List<StorageBackupJob> getRestoreJobs();

	StorageBackupJob getBackupJob(String guid);

	StorageBackupJob getRestoreJob(String guid);

	StorageBackupJob prepare(StorageBackupRequest req) throws IOException;

	void execute(StorageBackupJob job);
}
