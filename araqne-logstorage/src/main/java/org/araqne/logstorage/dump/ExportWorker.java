package org.araqne.logstorage.dump;

public interface ExportWorker extends Runnable {
	ExportTask getTask();
}
