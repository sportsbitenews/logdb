package org.araqne.logdb.client;

import java.util.ArrayList;
import java.util.List;

/**
 * 트랜스포머를 생성하는데 필요한 설정 명세를 표현합니다.
 * 
 * @author xeraph@eediom.com
 * 
 */
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
