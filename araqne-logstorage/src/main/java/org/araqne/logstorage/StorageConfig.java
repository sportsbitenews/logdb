package org.araqne.logstorage;

import java.util.ArrayList;
import java.util.List;

import org.araqne.api.CollectionTypeHint;
import org.araqne.api.FieldOption;

public class StorageConfig {
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

}
