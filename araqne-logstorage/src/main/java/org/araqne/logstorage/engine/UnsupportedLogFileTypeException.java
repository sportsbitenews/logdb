package org.araqne.logstorage.engine;

public class UnsupportedLogFileTypeException extends RuntimeException {

	public UnsupportedLogFileTypeException(String type) {
		super(type);
	}

	private static final long serialVersionUID = 1L;

}
