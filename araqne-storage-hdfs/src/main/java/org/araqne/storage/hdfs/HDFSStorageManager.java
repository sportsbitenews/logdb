package org.araqne.storage.hdfs;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.araqne.storage.api.URIResolver;

public interface HDFSStorageManager extends URIResolver {

	void start();

	void stop();
	
	List<HDFSCluster> getClusters();

	boolean removeCluster(String alias);

	boolean addCluster(List<String> confFiles, String alias);
}
