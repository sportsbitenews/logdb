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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.araqne.logdb.Session;

public class SessionImpl implements Session {
	private String guid;
	private String loginName;
	
	/**
	 * @since 1.0.0
	 */
	private boolean admin;

	private Date created;

	/**
	 * @since 2.4.53
	 */
	private Map<String, Object> props;
	
	public SessionImpl(String guid, String loginName, boolean admin) {
		this.guid = guid;
		this.loginName = loginName;
		this.admin = admin;
		this.created = new Date();
		
		props = new ConcurrentHashMap<String, Object>();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((guid == null) ? 0 : guid.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SessionImpl other = (SessionImpl) obj;
		if (guid == null) {
			if (other.guid != null)
				return false;
		} else if (!guid.equals(other.guid))
			return false;
		return true;
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
	public boolean isAdmin() {
		return admin;
	}

	@Override
	public Date getCreated() {
		return created;
	}

	@Override
	public Set<String> getPropertyKeys() {
		return props.keySet();
	}

	@Override
	public Object getProperty(String name) {
		return props.get(name);
	}

	@Override
	public void setProperty(String name, Object value) {
		props.put(name, value);
	}

	@Override
	public void unsetProperty(String name) {
		if(props.containsKey(name))
			props.remove(name);
	}

	@Override
	public String toString() {
		SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return loginName + ", login at " + f.format(created) + ", key " + guid;
	}
}
