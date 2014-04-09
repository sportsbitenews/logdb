package org.araqne.logstorage;
public interface LogWriteOnDiskCallback {
	void onWriteCompleted(LogWriteOnDiskCallbackArgs arg);
}