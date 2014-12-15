package org.araqne.logstorage;

public interface WriteFallback {
	boolean onLockFailure(String reason);
}
