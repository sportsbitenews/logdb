package org.araqne.logstorage;

public class WrongTimeTypeException extends IllegalStateException {
	private static final long serialVersionUID = 1L;

	public Object time;

	public WrongTimeTypeException(Object time) {
		this.time = time;
	}
}
