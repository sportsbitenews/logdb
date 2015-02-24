package org.araqne.logstorage.dump;

import java.util.List;

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

	/**
	 * @return guid of import task
	 */
	String beginImport(ImportRequest req);
	
	void cancelImport(String guid);

	List<DumpDriver> getDumpDrivers();

	void registerDriver(DumpDriver driver);

	void unregisterDriver(DumpDriver driver);

	List<ExportTabletTask> estimate(ExportRequest req);

}
