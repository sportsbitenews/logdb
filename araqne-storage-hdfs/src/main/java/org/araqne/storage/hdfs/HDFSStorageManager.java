package org.araqne.storage.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.araqne.storage.api.URIResolver;

public interface HDFSStorageManager extends URIResolver {

	void start();

	void stop();
	
	boolean addCluster(Configuration conf, String alias);
}
