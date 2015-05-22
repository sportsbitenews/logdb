package org.araqne.logstorage;

public class TableAlreadyExistsException extends RuntimeException {

	public TableAlreadyExistsException(String string) {
		super(string);
	}

}
