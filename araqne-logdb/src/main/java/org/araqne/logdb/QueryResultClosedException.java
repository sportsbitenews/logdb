package org.araqne.logdb;

public class QueryResultClosedException extends IllegalStateException {
	private static final long serialVersionUID = 1L;
	
	public QueryResultClosedException() {
		super("result writer is already closed");
	}
}
