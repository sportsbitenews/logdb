package org.araqne.logdb;

public interface ExternalAccountService {
	boolean verifyPassword(String loginName, String password);
}
