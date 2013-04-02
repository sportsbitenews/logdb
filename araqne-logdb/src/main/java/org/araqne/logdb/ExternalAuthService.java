package org.araqne.logdb;

public interface ExternalAuthService {
	String getName();

	boolean verifyUser(String loginName);

	boolean verifyPassword(String loginName, String password);
}
