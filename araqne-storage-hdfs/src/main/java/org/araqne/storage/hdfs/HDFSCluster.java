package org.araqne.storage.hdfs;

import org.apache.hadoop.fs.FileSystem;

public class HDFSCluster {
	private final FileSystem fs;
	private final String alias;
	private final String protocol;
	
	public HDFSCluster(FileSystem fs, String alias) {
		this.fs = fs;
		this.alias = alias;
		this.protocol = "hdfs"; // TODO : handle hftp, webhdfs
	}
	
	public String toString() {
		return protocol + "://" + alias;
	}
	
	public String getProtocol() {
		return protocol;
	}
	
	public FileSystem getFileSystem() {
		return fs;
	}
	
	public String getAlias() {
		return alias;
	}
	
	@Override
	public boolean equals(Object o) {
		if (!(o instanceof HDFSCluster))
			return false;
		
		// if FileSystems are identical, HDFSRoots are identical.
		HDFSCluster rhs = (HDFSCluster) o;
		return fs.equals(rhs.fs);
	}
}
