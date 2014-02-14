package org.araqne.storage.hdfs;

import java.util.List;

import org.araqne.confdb.CollectionName;

@CollectionName("storagecluster")
public class HDFSStorageClusterConfig {
	private String name;
	private List<String> confFiles;
	
	public HDFSStorageClusterConfig() {
	}
	
	public HDFSStorageClusterConfig(String name, List<String> confFiles) {
		this.name = name;
		this.confFiles = confFiles;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public List<String> getConfFiles() {
		return confFiles;
	}
	
	public void setConfFiles(List<String> confFiles) {
		this.confFiles = confFiles;
	}
}
