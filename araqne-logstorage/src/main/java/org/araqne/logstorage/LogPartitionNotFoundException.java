package org.araqne.logstorage;

@SuppressWarnings("serial")
public class LogPartitionNotFoundException extends RuntimeException {

	public LogPartitionNotFoundException(String msg) {
		super(msg);
	}

}
