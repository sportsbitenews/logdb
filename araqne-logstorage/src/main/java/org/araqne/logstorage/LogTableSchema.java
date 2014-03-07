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
package org.araqne.logstorage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.api.CollectionTypeHint;
import org.araqne.confdb.CollectionName;
import org.araqne.log.api.FieldDefinition;

@CollectionName("table")
public class LogTableSchema {
	private int id;
	private String name;

	/**
	 * Table may contains other field which is not specified here. Query
	 * designer can use these field definitions for search dialog rendering,
	 * lookup input/output mapping, etc.
	 */
	@CollectionTypeHint(FieldDefinition.class)
	private List<FieldDefinition> fieldDefinitions;

	private Map<String, Object> metadata;

	public LogTableSchema() {
	}

	public LogTableSchema(int id, String name) {
		this.id = id;
		this.name = name;
		this.metadata = new HashMap<String, Object>();
	}

	public int getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<FieldDefinition> getFieldDefinitions() {
		return fieldDefinitions;
	}

	public void setFieldDefinitions(List<FieldDefinition> fieldDefinitions) {
		this.fieldDefinitions = fieldDefinitions;
	}

	public Map<String, Object> getMetadata() {
		return metadata;
	}
}
