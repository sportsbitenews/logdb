/*
 * Copyright 2010 NCHOVY
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
package org.araqne.log.api.impl;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.araqne.api.FieldOption;
import org.araqne.confdb.CollectionName;
import org.araqne.log.api.Logger;

@CollectionName("logger")
public class LoggerConfig {
	/**
	 * schema version for migration
	 */
	private int version = 1;

	private String factoryNamespace;
	private String factoryName;
	private String namespace;
	private String fullname;
	private String name;
	private String description;

	/**
	 * ignore since version 2 (migrated to state file)
	 */
	private boolean isRunning;

	/**
	 * ignore since version 2 (migrated to state file)
	 */
	private boolean isPending;

	/**
	 * @since 2.4.0
	 */
	private boolean manualStart;

	/**
	 * ignore since version 2 (migrated to state file)
	 */
	private int interval;

	/**
	 * ignore since version 2 (migrated to state file)
	 */
	@FieldOption(skip = true)
	private long count;

	/**
	 * ignore since version 2 (migrated to state file)
	 */
	@FieldOption(skip = true)
	private Date lastLogDate;

	private Map<String, String> configs = new HashMap<String, String>();

	public LoggerConfig() {
	}

	public LoggerConfig(Logger logger) {
		this.factoryNamespace = logger.getFactoryNamespace();
		this.factoryName = logger.getFactoryName();
		this.namespace = logger.getNamespace();
		this.fullname = logger.getFullName();
		this.name = logger.getName();
		this.description = logger.getDescription();
		this.isRunning = logger.isRunning();
		this.isPending = logger.isPending();
		this.manualStart = logger.isManualStart();
		this.interval = logger.getInterval();
		this.count = logger.getLogCount();
		this.lastLogDate = logger.getLastLogDate();
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public String getFactoryNamespace() {
		return factoryNamespace;
	}

	public void setFactoryNamespace(String factoryNamespace) {
		this.factoryNamespace = factoryNamespace;
	}

	public String getFactoryName() {
		return factoryName;
	}

	public void setFactoryName(String factoryName) {
		this.factoryName = factoryName;
	}

	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public String getFullname() {
		return fullname;
	}

	public void setFullname(String fullname) {
		this.fullname = fullname;
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

	@Deprecated
	public boolean isRunning() {
		return isRunning;
	}

	@Deprecated
	public void setRunning(boolean isRunning) {
		this.isRunning = isRunning;
	}

	@Deprecated
	public boolean isPending() {
		return isPending;
	}

	@Deprecated
	public void setPending(boolean isPending) {
		this.isPending = isPending;
	}

	public boolean isManualStart() {
		return manualStart;
	}

	public void setManualStart(boolean manualStart) {
		this.manualStart = manualStart;
	}

	@Deprecated
	public int getInterval() {
		return interval;
	}

	@Deprecated
	public void setInterval(int interval) {
		this.interval = interval;
	}

	@Deprecated
	public long getCount() {
		return count;
	}

	@Deprecated
	public void setCount(long count) {
		this.count = count;
	}

	public Date getLastLogDate() {
		return lastLogDate;
	}

	public void setLastLogDate(Date lastLogDate) {
		this.lastLogDate = lastLogDate;
	}

	public void setConfigs(Map<String, String> configs) {
		this.configs = configs;
	}

	public Map<String, String> getConfigs() {
		return configs;
	}

	@Override
	public String toString() {
		return "factory=" + factoryNamespace + "\\" + factoryName + ", fullname=" + fullname + ", running=" + isRunning
				+ ", interval=" + interval + ", configs=" + configs;
	}
}
