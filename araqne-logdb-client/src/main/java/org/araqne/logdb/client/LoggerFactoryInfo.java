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
package org.araqne.logdb.client;

import java.util.ArrayList;
import java.util.List;

/**
 * 로그 수집기를 생성하는데 필요한 설정 명세를 표현합니다.
 * 
 * @author xeraph@eediom.com
 * 
 */
public class LoggerFactoryInfo {
	private String fullName;
	private String displayName;
	private String namespace;
	private String name;
	private String description;
	private List<ConfigSpec> configSpecs = new ArrayList<ConfigSpec>();

	public String getFullName() {
		return fullName;
	}

	public void setFullName(String fullName) {
		this.fullName = fullName;
	}

	public String getDisplayName() {
		return displayName;
	}

	public void setDisplayName(String displayName) {
		this.displayName = displayName;
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

	public List<ConfigSpec> getConfigSpecs() {
		return configSpecs;
	}

	public void setConfigSpecs(List<ConfigSpec> configSpecs) {
		this.configSpecs = configSpecs;
	}

	@Override
	public String toString() {
		return String.format("fullname=%s, type=%s, description=%s", fullName, displayName, description);
	}
}
