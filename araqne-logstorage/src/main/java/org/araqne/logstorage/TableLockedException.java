package org.araqne.logstorage;

public class TableLockedException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public TableLockedException(String msg) {
		super(msg);
	}

}
