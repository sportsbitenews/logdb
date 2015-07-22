package org.araqne.logstorage.dump;

public interface ImportWorker extends Runnable {
	ImportTask getTask();
}
