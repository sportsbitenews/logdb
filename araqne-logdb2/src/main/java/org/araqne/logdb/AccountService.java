/*
 * Copyright 2013 Eediom Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.araqne.logdb;

import java.util.List;
import java.util.Set;

public interface AccountService {
	List<Session> getSessions();

	Session getSession(String guid);

	/**
	 * @since 1.2.5
	 */
	Session newSession(String loginName);

	Session login(String loginName, String hash, String nonce);

	Session login(String loginName, String password);

	void logout(Session session);

	Set<String> getAccountNames();

	/**
	 * @since 1.3.8
	 */
	Account getAccount(String name);

	boolean verifyPassword(String loginName, String password);

	void createAccount(Session session, String loginName, String password);

	void changePassword(Session session, String loginName, String password);

	void removeAccount(Session session, String loginName);

	boolean checkPermission(Session session, String tableName, Permission permission);

	/**
	 * @since 1.0.0
	 */
	boolean isAdmin(String loginName);

	/**
	 * @since 1.0.0
	 */
	void grantAdmin(Session session, String loginName);

	/**
	 * @since 1.0.0
	 */
	void revokeAdmin(Session session, String loginName);

	List<Privilege> getPrivileges(Session session, String loginName);

	void setPrivileges(Session session, String loginName, List<Privilege> privileges);

	void grantPrivilege(Session session, String loginName, String tableName, Permission... permissions);

	void revokePrivilege(Session session, String loginName, String tableName, Permission... permissions);

	ExternalAuthService getUsingAuthService();

	void useAuthService(String name);

	List<ExternalAuthService> getAuthServices();

	ExternalAuthService getAuthService(String name);

	void registerAuthService(ExternalAuthService auth);

	void unregisterAuthService(ExternalAuthService auth);

	/**
	 * @since 0.17.0
	 */
	void addListener(SessionEventListener listener);

	/**
	 * @since 0.17.0
	 */
	void removeListener(SessionEventListener listener);
}
