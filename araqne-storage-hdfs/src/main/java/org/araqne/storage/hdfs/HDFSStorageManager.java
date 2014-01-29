package org.araqne.storage.hdfs;

import org.araqne.storage.api.URIResolver;

public interface HDFSStorageManager extends URIResolver {

	void start();

	void stop();
}
