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

import java.text.SimpleDateFormat;
import java.util.Date;

import org.araqne.logdb.Session;

public class SessionImpl implements Session {
	private String guid;
	private String loginName;
	private Date created;

	public SessionImpl(String guid, String loginName) {
		this.guid = guid;
		this.loginName = loginName;
		this.created = new Date();
	}

	@Override
	public String getGuid() {
		return guid;
	}

	@Override
	public String getLoginName() {
		return loginName;
	}

	@Override
	public Date getCreated() {
		return created;
	}

	@Override
	public String toString() {
		SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return loginName + ", login at " + f.format(created) + ", key " + guid;
	}
}
