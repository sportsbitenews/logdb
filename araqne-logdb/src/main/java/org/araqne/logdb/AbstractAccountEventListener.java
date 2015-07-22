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

/**
 * Empty callback implementations for override.
 * 
 * @author xeraph
 * 
 */
public class AbstractAccountEventListener implements AccountEventListener {

	@Override
	public void onCreateAccount(Session session, Account account) {
	}

	@Override
	public void onRemoveAccount(Session session, Account account) {
	}

	/**
	 * @since 2.6.34
	 */
	@Override
	public void onCreateSecurityGroup(Session session, SecurityGroup group) {
	}

	/**
	 * @since 2.6.34
	 */
	@Override
	public void onUpdateSecurityGroup(Session session, SecurityGroup group) {
	}

	/**
	 * @since 2.6.34
	 */
	@Override
	public void onRemoveSecurityGroup(Session session, SecurityGroup group) {
	}

	@Override
	public void onGrantAdmin(Session session, Account account) {
	}

	@Override
	public void onRevokeAdmin(Session session, Account account) {
	}

	@Override
	public void onGrantPrivilege(Session session, String loginName, String tableName, Permission... permissions) {
	}

	@Override
	public void onRevokePrivilege(Session session, String loginName, String tableName, Permission... permissions) {
	}
}
