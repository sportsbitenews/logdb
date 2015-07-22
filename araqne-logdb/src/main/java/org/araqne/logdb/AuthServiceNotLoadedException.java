package org.araqne.logdb;

public class AuthServiceNotLoadedException extends RuntimeException {
	private String selectedExternalAuth;

	public AuthServiceNotLoadedException(String selectedExternalAuth) {
		this.selectedExternalAuth = selectedExternalAuth;
	}
	
	public String getAuthService() {
		return selectedExternalAuth;
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

}
