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
package org.araqne.logdb.impl;

import java.util.ArrayList;
import java.util.List;

import org.araqne.api.CollectionTypeHint;
import org.araqne.confdb.CollectionName;

@CollectionName("accounts")
public class Account {
	private String loginName;

	private String salt;

	// salt hashed password (hex format)
	private String password;

	private String hashType = "sha1";

	@CollectionTypeHint(String.class)
	private List<String> readableTables = new ArrayList<String>();

	public Account() {
	}

	public Account(String loginName, String salt, String password) {
		this.loginName = loginName;
		this.salt = salt;
		this.password = password;
	}

	public String getLoginName() {
		return loginName;
	}

	public void setLoginName(String loginName) {
		this.loginName = loginName;
	}

	public String getSalt() {
		return salt;
	}

	public void setSalt(String salt) {
		this.salt = salt;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getHashType() {
		return hashType;
	}

	public void setHashType(String hashType) {
		this.hashType = hashType;
	}

	public List<String> getReadableTables() {
		return readableTables;
	}

	public void setReadableTables(List<String> readableTables) {
		this.readableTables = readableTables;
	}
}
