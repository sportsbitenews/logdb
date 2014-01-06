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

import java.util.HashMap;
import java.util.Map;

public class LoggerSpecification {
	private String namespace;
	private String name;
	private String description;

	/**
	 * @since 2.4.0
	 */
	private boolean manualStart;

	private Map<String, String> config = new HashMap<String, String>();

	public LoggerSpecification() {
	}

	public LoggerSpecification(String namespace, String name, String description, Map<String, String> config) {
		this.namespace = namespace;
		this.name = name;
		this.description = description;
		this.config = config;
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

	/**
	 * @since 2.4.0
	 */
	public boolean isManualStart() {
		return manualStart;
	}

	/**
	 * @since 2.4.0
	 */
	public void setManualStart(boolean manualStart) {
		this.manualStart = manualStart;
	}

	public Map<String, String> getConfig() {
		return config;
	}

	public void setConfig(Map<String, String> config) {
		this.config = config;
	}
}
