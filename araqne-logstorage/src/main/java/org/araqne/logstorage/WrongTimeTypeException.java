package org.araqne.logstorage;

public class WrongTimeTypeException extends IllegalStateException {
	public Object time;
	public WrongTimeTypeException(Object time) {
		this.time = time;
	}
}
