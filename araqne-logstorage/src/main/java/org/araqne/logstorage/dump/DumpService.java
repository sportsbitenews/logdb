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

}
