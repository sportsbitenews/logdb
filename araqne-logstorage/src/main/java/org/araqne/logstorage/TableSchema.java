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
package org.araqne.logstorage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.api.CollectionTypeHint;
import org.araqne.api.FieldOption;
import org.araqne.confdb.CollectionName;
import org.araqne.log.api.FieldDefinition;

@CollectionName("table")
public class TableSchema {
	/**
	 * unique table name in a node
	 */
	@FieldOption(nullable = false)
	private String name;

	/**
	 * table id for path generation (name may contains character which is not
	 * allowed at target storage filesystem)
	 */
	@FieldOption(nullable = false)
	private int id;

	@FieldOption(nullable = true)
	private String basePath;

	@FieldOption(nullable = false)
	private String storageEngine;

	@CollectionTypeHint(TableConfig.class)
	private List<TableConfig> storageConfigs = new ArrayList<TableConfig>();

	private String replicator;

	@CollectionTypeHint(TableConfig.class)
	private List<TableConfig> replicatorConfigs = new ArrayList<TableConfig>();

	/**
	 * Table may contains other field which is not specified here. Query
	 * designer can use these field definitions for search dialog rendering,
	 * lookup input/output mapping, etc.
	 */
	@CollectionTypeHint(FieldDefinition.class)
	private List<FieldDefinition> fieldDefinitions;

	private Map<String, String> metadata = new HashMap<String, String>();

	public TableSchema() {
	}

	public TableSchema(String name, String storageEngine) {
		this.name = name;
		this.storageEngine = storageEngine;
	}

	public TableSchema clone() {
		TableSchema c = new TableSchema();
		c.setName(name);
		c.setId(id);
		c.setBasePath(basePath);
		c.setStorageEngine(storageEngine);
		c.setStorageConfigs(cloneConfigs(storageConfigs));
		c.setReplicator(replicator);
		c.setReplicatorConfigs(cloneConfigs(replicatorConfigs));
		c.setFieldDefinitions(cloneFieldDefinitions(fieldDefinitions));
		c.setMetadata(new HashMap<String, String>(metadata));

		return c;
	}

	private List<FieldDefinition> cloneFieldDefinitions(List<FieldDefinition> l) {
		if (l == null)
			return null;

		List<FieldDefinition> cloned = new ArrayList<FieldDefinition>();
		for (FieldDefinition d : l)
			cloned.add(FieldDefinition.parse(d.toString()));
		return cloned;
	}

	private List<TableConfig> cloneConfigs(List<TableConfig> l) {
		List<TableConfig> cloned = new ArrayList<TableConfig>();
		for (TableConfig config : l)
			cloned.add(config.clone());
		return cloned;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getBasePath() {
		return basePath;
	}

	public void setBasePath(String basePath) {
		this.basePath = basePath;
	}

	public String getStorageEngine() {
		return storageEngine;
	}

	public void setStorageEngine(String storageEngine) {
		this.storageEngine = storageEngine;
	}

	public List<TableConfig> getStorageConfigs() {
		return storageConfigs;
	}

	public TableConfig getStorageConfig(String key) {
		for (TableConfig c : storageConfigs)
			if (c.getKey().equals(key))
				return c;
		return null;
	}

	public void setStorageConfigs(List<TableConfig> storageConfigs) {
		this.storageConfigs = storageConfigs;
	}

	public String getReplicator() {
		return replicator;
	}

	public void setReplicator(String replicator) {
		this.replicator = replicator;
	}

	public List<TableConfig> getReplicatorConfigs() {
		return replicatorConfigs;
	}

	public void setReplicatorConfigs(List<TableConfig> replicatorConfigs) {
		this.replicatorConfigs = replicatorConfigs;
	}

	public List<FieldDefinition> getFieldDefinitions() {
		if (fieldDefinitions == null)
			return null;
		return new ArrayList<FieldDefinition>(fieldDefinitions);
	}

	/**
	 * update table field definitions
	 * 
	 * @param tableName
	 *            existing table name
	 * @param fields
	 *            field definitions or null
	 * @since 2.5.1
	 */
	public void setFieldDefinitions(List<FieldDefinition> fieldDefinitions) {
		this.fieldDefinitions = fieldDefinitions;
	}

	public Map<String, String> getMetadata() {
		return metadata;
	}

	public void setMetadata(Map<String, String> metadata) {
		this.metadata = metadata;
	}
}
