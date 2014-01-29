package org.araqne.storage.hdfs;

import org.apache.hadoop.fs.FileSystem;

class HDFSRoot {
	private final FileSystem fs;
	private final String alias;
	private final String protocol;
	
	public HDFSRoot(FileSystem fs, String alias) {
		this.fs = fs;
		this.alias = alias;
		this.protocol = "hdfs"; // TODO : handle hftp, webhdfs
	}
	
	public String toString() {
		return protocol + "://" + alias;
	}
	
	public FileSystem getFileSystem() {
		return fs;
	}
	
	public String getAlias() {
		return alias;
	}
	
	@Override
	public boolean equals(Object o) {
		if (!(o instanceof HDFSRoot))
			return false;
		
		// if FileSystems are identical, HDFSRoots are identical.
		HDFSRoot rhs = (HDFSRoot) o;
		return fs.equals(rhs.fs);
	}
}
