/**
 * Copyright 2015 Eediom Inc.
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

public interface AccountEventListener {
	void onCreateAccount(Session session, Account account);

	void onRemoveAccount(Session session, Account account);

	/**
	 * @since 2.8.0
	 */
	void onRemoveAccounts(Session session, List<Account> accounts);

	/**
	 * @since 2.6.34
	 */
	void onCreateSecurityGroup(Session session, SecurityGroup group);

	/**
	 * @since 2.6.34
	 */
	void onUpdateSecurityGroup(Session session, SecurityGroup group);

	/**
	 * @since 2.6.34
	 */
	void onRemoveSecurityGroup(Session session, SecurityGroup group);

	void onGrantAdmin(Session session, Account account);

	void onRevokeAdmin(Session session, Account account);

	void onGrantPrivilege(Session session, String loginName, String tableName, Permission... permissions);

	void onRevokePrivilege(Session session, String loginName, String tableName, Permission... permissions);
}
