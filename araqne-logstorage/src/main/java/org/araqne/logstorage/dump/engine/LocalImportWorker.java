package org.araqne.logstorage.dump.engine;

import java.io.File;
import java.io.IOException;

import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.dump.DumpService;
import org.araqne.logstorage.dump.ImportRequest;
import org.araqne.logstorage.dump.ImportTask;
import org.araqne.logstorage.dump.ImportWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalImportWorker implements ImportWorker {
	
	private final Logger slog = LoggerFactory.getLogger(LocalImportWorker.class);

	private ImportRequest req;
	private ImportTask task;
	
	private DumpService dumpService;
	private LogStorage storage;
	
	private File path;

	public LocalImportWorker(ImportRequest req, DumpService dumpService, LogStorage storage) {
		this.req = req;
		this.dumpService = dumpService;
		this.storage = storage;
		this.task = new ImportTask(req.getGuid());
		this.path = new File(req.getParams().get("path"));
	}

	@Override
	public ImportTask getTask() {
		return task;
	}

	@Override
	public void run() {
		try {
			dumpService.readManifest("local", req.getParams());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	

}
