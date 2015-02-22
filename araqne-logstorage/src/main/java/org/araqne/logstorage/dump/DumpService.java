package org.araqne.logstorage.dump;

import java.util.List;

public interface DumpService {
	List<ExportTask> getExportTasks();

	List<ImportTask> getImportTasks();

	/**
	 * @return guid of export task
	 */
	String beginExport(ExportRequest req);

	/**
	 * @return guid of import task
	 */
	String beginImport(ImportRequest req);

	List<DumpDriver> getDumpDrivers();

	void registerDriver(DumpDriver driver);

	void unregisterDriver(DumpDriver driver);

	List<ExportTabletTask> estimate(ExportRequest req);

}
