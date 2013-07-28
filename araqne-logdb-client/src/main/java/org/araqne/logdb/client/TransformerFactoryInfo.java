package org.araqne.logdb.client;

import java.util.ArrayList;
import java.util.List;

public class TransformerFactoryInfo {
	private String name;
	private String displayName;
	private String description;
	private List<ConfigSpec> configSpecs = new ArrayList<ConfigSpec>();

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDisplayName() {
		return displayName;
	}

	public void setDisplayName(String displayName) {
		this.displayName = displayName;
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
		return "name=" + name + ", description=" + description + ", config specs=" + configSpecs;
	}
}
