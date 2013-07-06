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
package org.araqne.log.api;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class LoggerSpecification {
	private String namespace;
	private String name;
	private String description;
	private long logCount;
	private boolean isPassive;
	private Date lastLogDate;
	private int interval;
	private Map<String, String> config = new HashMap<String, String>();

	public LoggerSpecification() {
	}

	public LoggerSpecification(String namespace, String name, String description, long logCount, Date lastLogDate,
			int interval, Map<String, String> config) {
		this(namespace, name, description, logCount, lastLogDate, interval, config, false);
	}

	public LoggerSpecification(String namespace, String name, String description, long logCount, Date lastLogDate,
			int interval, Map<String, String> config, boolean isPassive) {
		this.namespace = namespace;
		this.name = name;
		this.description = description;
		this.logCount = logCount;
		this.lastLogDate = lastLogDate;
		this.interval = interval;
		this.config = config;
		this.isPassive = isPassive;
	}

	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
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

	public long getLogCount() {
		return logCount;
	}

	public void setLogCount(long logCount) {
		this.logCount = logCount;
	}

	public boolean isPassive() {
		return isPassive;
	}

	public void setPassive(boolean isPassive) {
		this.isPassive = isPassive;
	}

	public Date getLastLogDate() {
		return lastLogDate;
	}

	public void setLastLogDate(Date lastLogDate) {
		this.lastLogDate = lastLogDate;
	}

	public int getInterval() {
		return interval;
	}

	public void setInterval(int interval) {
		this.interval = interval;
	}

	public Map<String, String> getConfig() {
		return config;
	}

	public void setConfig(Map<String, String> config) {
		this.config = config;
	}
}
