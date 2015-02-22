package org.araqne.logstorage.dump.engine;

import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.dump.DumpService;
import org.araqne.logstorage.dump.ImportRequest;
import org.araqne.logstorage.dump.ImportTask;
import org.araqne.logstorage.dump.ImportWorker;

public class LocalImportWorker implements ImportWorker {

	private ImportRequest req;
	private DumpService dumpService;
	private LogStorage storage;

	public LocalImportWorker(ImportRequest req, DumpService dumpService, LogStorage storage) {
		this.req = req;
		this.dumpService = dumpService;
		this.storage = storage;
	}

	@Override
	public ImportTask getTask() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub

	}
}
