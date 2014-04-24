/*
 * Copyright 2014 Eediom Inc.
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 파서 인스턴스 정보를 표현합니다.
 * 
 * @since 0.8.0
 * @author xeraph@eediom.com
 */
public class ParserInfo {
	private String name;
	private String factoryName;

	// @since 0.9.0
	private List<FieldInfo> fieldDefinitions;

	private Map<String, String> configs = new HashMap<String, String>();

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getFactoryName() {
		return factoryName;
	}

	public void setFactoryName(String factoryName) {
		this.factoryName = factoryName;
	}

	public List<FieldInfo> getFieldDefinitions() {
		return fieldDefinitions;
	}

	public void setFieldDefinitions(List<FieldInfo> fieldDefinitions) {
		this.fieldDefinitions = fieldDefinitions;
	}

	public Map<String, String> getConfigs() {
		return configs;
	}

	public void setConfigs(Map<String, String> configs) {
		this.configs = configs;
	}

	@Override
	public String toString() {
		return "name=" + name + ", factory=" + factoryName + ", configs=" + configs;
	}
}
