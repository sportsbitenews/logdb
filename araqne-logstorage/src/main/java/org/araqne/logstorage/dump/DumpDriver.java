package org.araqne.logstorage.dump;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public interface DumpDriver {
	String getType();

	String getName(Locale locale);

	String getDescription(Locale locale);

	List<DumpConfigSpec> getExportSpecs();

	List<DumpConfigSpec> getImportSpecs();

	DumpManifest readManifest(Map<String, String> params) throws IOException;

	ExportWorker newExportWorker(ExportRequest req);

	ImportWorker newImportWorker(ImportRequest req);
}
