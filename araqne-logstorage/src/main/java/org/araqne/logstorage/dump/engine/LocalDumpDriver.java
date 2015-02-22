package org.araqne.logstorage.dump.engine;

import static org.araqne.logstorage.dump.DumpConfigSpec.t;

import java.util.Arrays;
import java.util.List;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.dump.DumpConfigSpec;
import org.araqne.logstorage.dump.DumpDriver;
import org.araqne.logstorage.dump.DumpService;
import org.araqne.logstorage.dump.ExportRequest;
import org.araqne.logstorage.dump.ExportWorker;
import org.araqne.logstorage.dump.ImportRequest;
import org.araqne.logstorage.dump.ImportWorker;

@Component(name = "logstorage-local-dump-driver")
public class LocalDumpDriver implements DumpDriver {

	@Requires
	private LogStorage storage;

	@Requires
	private DumpService dumpService;

	@Validate
	public void start() {
		dumpService.registerDriver(this);
	}

	@Invalidate
	public void stop() {
		if (dumpService != null)
			dumpService.unregisterDriver(this);
	}

	@Override
	public String getName() {
		return "local";
	}

	@Override
	public List<DumpConfigSpec> getExportSpecs() {
		DumpConfigSpec path = new DumpConfigSpec("path", t("Path", "경로"), t("Export file path", "덤프 파일 경로"), true);
		return Arrays.asList(path);
	}

	@Override
	public List<DumpConfigSpec> getImportSpecs() {
		DumpConfigSpec path = new DumpConfigSpec("path", t("Path", "경로"), t("Export file path", "덤프 파일 경로"), true);
		return Arrays.asList(path);
	}

	@Override
	public ExportWorker newExportWorker(ExportRequest req) {
		return new LocalExportWorker(req, dumpService, storage);
	}

	@Override
	public ImportWorker newImportWorker(ImportRequest req) {
		return new LocalImportWorker(req, dumpService, storage);
	}

}
