package org.araqne.storage.hdfs;

import java.io.IOException;
import java.util.Arrays;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.araqne.storage.api.FilePath;
import org.araqne.storage.api.StorageManager;
import org.araqne.storage.api.StorageUtil;

@Component(name = "araqne-hdfs-storage-manager")
@Provides
public class HDFSStorageManagerImpl implements HDFSStorageManager {
	private final String[] protocols = new String[] {"hdfs", "hftp"};

	// FIXME : handle multiple alias and FileSystem
    private HDFSRoot root;
    
	@Requires
	private StorageManager storageManager;
    
	@Validate
	@Override
	public void start() {
        Configuration conf = new Configuration();
        conf.addResource(new Path("/home/everclear/eediom/hadoop/hadoop-common/fs-impl.xml"));
        conf.addResource(new Path("/data/hadoop-2.2.0/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/data/hadoop-2.2.0/etc/hadoop/hdfs-site.xml"));
        conf.addResource(new Path("/data/hadoop-2.2.0/etc/hadoop/mapred-site.xml"));
        conf.setClassLoader(Configuration.class.getClassLoader());
        FileSystem hdfs = null; 
        try {
        	Class<?> zz = conf.getClass("fs.hdfs.impl", null); // FIXME : 
        	hdfs = FileSystem.get(conf);
        } catch (IOException e) {
        	e.printStackTrace();
        }
        
        root = new HDFSRoot(hdfs, "localhost:9000");
        
        storageManager.addURIResolver(this);
	}
	
	@Invalidate
	@Override
	public void stop() {
		
	}
	
	@Override
	public FilePath resolveFilePath(String path) {
		String protocolString = StorageUtil.extractProtocol(path);
		// FIXME : handle multiple alias and FileSystem
		if (protocolString == null)
			return null;
		
		String protocol = protocolString.substring(0, protocolString.length() - 3);
		if (!Arrays.asList(protocols).contains(protocol))
			return null;
		
		int rootStartIdx = path.indexOf('/', protocolString.length());
		// TODO : get specified root from rootString
		String rootString = path.substring(0, rootStartIdx);
		String subPath = path.substring(rootStartIdx);
		
		return new HDFSFilePath(root, subPath);
	}
	


	@Override
	public String[] getProtocols() {
		return protocols;
	}

}
