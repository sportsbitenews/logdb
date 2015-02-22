package org.araqne.logstorage.dump;

import java.util.List;

public interface DumpDriver {
	String getName();

	List<DumpConfigSpec> getExportSpecs();

	List<DumpConfigSpec> getImportSpecs();

	ExportWorker newExportWorker(ExportRequest req);

	ImportWorker newImportWorker(ImportRequest req);
}
