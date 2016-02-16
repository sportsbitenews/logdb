package org.araqne.storage.crypto;

public class LogCryptoException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public LogCryptoException(String message) {
		super(message);
	}
	
	public LogCryptoException(String message, Throwable cause) {
		super(message, cause);
	}
}
