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

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.araqne.api.CollectionTypeHint;
import org.araqne.api.FieldOption;
import org.araqne.confdb.CollectionName;
import org.araqne.msgbus.Marshalable;

/**
 * @since 2.6.34
 * @author xeraph
 * 
 */
@CollectionName("security_groups")
public class SecurityGroup implements Marshalable {
	@FieldOption(nullable = false)
	private String guid = UUID.randomUUID().toString();

	@FieldOption(nullable = false)
	private String name;

	private String description;

	@CollectionTypeHint(String.class)
	private Set<String> accounts = new HashSet<String>();

	@CollectionTypeHint(String.class)
	private Set<String> readableTables = new HashSet<String>();

	@FieldOption(nullable = false)
	private Date created = new Date();

	@FieldOption(nullable = false)
	private Date updated = new Date();

	public SecurityGroup clone() {
		SecurityGroup c = new SecurityGroup();
		c.guid = guid;
		c.name = name;
		c.description = description;
		c.accounts = new HashSet<String>(accounts);
		c.readableTables = new HashSet<String>(readableTables);
		c.created = created;
		c.updated = updated;
		return c;
	}

	public String getGuid() {
		return guid;
	}

	public void setGuid(String guid) {
		this.guid = guid;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Set<String> getAccounts() {
		return accounts;
	}

	public void setAccounts(Set<String> accounts) {
		this.accounts = accounts;
	}

	public Set<String> getReadableTables() {
		return readableTables;
	}

	public void setReadableTables(Set<String> readableTables) {
		this.readableTables = readableTables;
	}

	public Date getCreated() {
		return created;
	}

	public void setCreated(Date created) {
		this.created = created;
	}

	public Date getUpdated() {
		return updated;
	}

	public void setUpdated(Date updated) {
		this.updated = updated;
	}

	@Override
	public Map<String, Object> marshal() {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("guid", guid);
		m.put("name", name);
		m.put("description", description);
		m.put("accounts", accounts);
		m.put("table_names", readableTables);
		m.put("created", created);
		m.put("updated", updated);
		return m;
	}

	@Override
	public String toString() {
		return "guid=" + guid + ", name=" + name + ", accounts=" + accounts + ", tables=" + readableTables;
	}
}
