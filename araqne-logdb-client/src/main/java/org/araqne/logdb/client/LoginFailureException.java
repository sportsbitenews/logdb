package org.araqne.logdb.client;

public class LoginFailureException extends RuntimeException {
	private String errorCode;

	public LoginFailureException(String errorCode) {
		super(errorCode);
		this.errorCode = errorCode;
	}
	
	public String errorCode() {
		return errorCode;
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

}
