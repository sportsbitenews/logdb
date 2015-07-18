/**
 * Copyright 2015 Eediom Inc.
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
package org.araqne.logstorage.dump;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface DumpService {
	List<ExportTask> getExportTasks();

	ExportTask getExportTask(String guid);

	List<ImportTask> getImportTasks();

	ImportTask getImportTask(String guid);

	/**
	 * @return guid of export task
	 */
	String beginExport(ExportRequest req);

	void cancelExport(String guid);

	DumpManifest readManifest(String driverType, Map<String, String> params) throws IOException;

	/**
	 * @return guid of import task
	 */
	String beginImport(ImportRequest req);

	void cancelImport(String guid);

	List<DumpDriver> getDumpDrivers();

	DumpDriver getDumpDriver(String name);

	void registerDriver(DumpDriver driver);

	void unregisterDriver(DumpDriver driver);

	List<ExportTabletTask> estimate(ExportRequest req);

	void addListener(DumpEventListener listener);

	void removeListener(DumpEventListener listener);
}
