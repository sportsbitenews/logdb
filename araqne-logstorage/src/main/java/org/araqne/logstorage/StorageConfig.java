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
import org.araqne.msgbus.Marshalable;

public class StorageConfig implements Marshalable {
	/**
	 * engine type
	 */
	@FieldOption(nullable = false)
	private String type;

	@FieldOption(nullable = true)
	private String basePath;

	@CollectionTypeHint(TableConfig.class)
	private List<TableConfig> configs = new ArrayList<TableConfig>();

	public StorageConfig() {
	}

	public StorageConfig(String type) {
		this.type = type;
	}

	public StorageConfig(String type, String basePath) {
		this.type = type;
		this.basePath = basePath;
	}

	public StorageConfig clone() {
		StorageConfig c = new StorageConfig();
		c.setType(type);
		c.setBasePath(basePath);
		c.setConfigs(clone(configs));
		return c;
	}

	private List<TableConfig> clone(List<TableConfig> l) {
		List<TableConfig> cloned = new ArrayList<TableConfig>();
		for (TableConfig config : l)
			cloned.add(config.clone());
		return cloned;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getBasePath() {
		return basePath;
	}

	public void setBasePath(String basePath) {
		this.basePath = basePath;
	}

	public List<TableConfig> getConfigs() {
		return configs;
	}

	public TableConfig getConfig(String key) {
		for (TableConfig c : configs)
			if (c.getKey().equals(key))
				return c;
		return null;
	}

	public void setConfigs(List<TableConfig> configs) {
		this.configs = configs;
	}

	@Override
	public Map<String, Object> marshal() {
		Map<String, Object> cm = new HashMap<String, Object>();
		for (TableConfig c : this.configs) {
			if (c.getValues().size() < 2)
				cm.put(c.getKey(), c.getValue());
			else
				cm.put(c.getKey(), c.getValues());
		}
		
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("type", type);
		m.put("base_path", basePath);
		m.put("configs", cm);
		return m;
	}
}
