package org.araqne.storage.hdfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Component(name = "araqne-hdfs-storage-manager")
@Provides
public class HDFSStorageManagerImpl implements HDFSStorageManager {
	private final Logger logger = LoggerFactory.getLogger("org.araqne.storage.hdfs.HDFSStorageManager");
	
	private final String[] protocols = new String[] {"hdfs"};

    private List<HDFSCluster> clusters = new ArrayList<HDFSCluster>();
    
	@Requires
	private StorageManager storageManager;
    
	@Validate
	@Override
	public void start() {
		// TODO : Remove this hard-coded configuration
        Configuration conf = new Configuration();
        conf.addResource(new Path("/data/hadoop-2.2.0/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/data/hadoop-2.2.0/etc/hadoop/hdfs-site.xml"));

        addCluster(conf, "localhost:9000");
        // XXX : hard coded end
        
        storageManager.addURIResolver(this);
	}
	
	@Invalidate
	@Override
	public void stop() {
		clusters.clear();
	}
	
	@Override
	public FilePath resolveFilePath(String path) {
		String protocolString = StorageUtil.extractProtocol(path);

		if (protocolString == null)
			return null;
		
		String protocol = protocolString.substring(0, protocolString.length() - 3);
		if (!Arrays.asList(protocols).contains(protocol))
			return null;
		
		int rootStartIdx = path.indexOf('/', protocolString.length());
		String host = path.substring(protocolString.length(), rootStartIdx);
		String subPath = path.substring(rootStartIdx);
		
		for (HDFSCluster root : clusters) {
			if (root.getProtocol().equals(protocol) && root.getAlias().equals(host)) {
				return new HDFSFilePath(root, subPath);
			}
		}
		
		return null;
	}
	


	@Override
	public String[] getProtocols() {
		return protocols;
	}

	@Override
	public boolean addCluster(Configuration conf, String alias) {
        conf.setIfUnset("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.setClassLoader(Configuration.class.getClassLoader());

        FileSystem hdfs = null; 
        try {
        	hdfs = FileSystem.newInstance(conf);
        } catch (IOException e) {
        	logger.error("araqne storage hdfs : cannot get FileSystem - ", e);
        }
        
        if (hdfs == null)
        	return false;
        
		String protocol = "hdfs";
		String protocolString = StorageUtil.extractProtocol(alias);

		if (protocolString != null) {
			protocol = protocolString.substring(0, protocolString.length() - 3);
			
			if (!Arrays.asList(protocols).contains(protocol))
				return false;
			
			alias = alias.substring(protocolString.length());
		}
		
		for (HDFSCluster cluster : clusters) {
			if (cluster.getAlias().equals(alias) || cluster.getFileSystem().equals(hdfs)) {
				logger.warn("araqne storage hdfs : already exists - " + cluster.toString());
				return false;
			}
		}

		// TODO: handle protocol
		clusters.add(new HDFSCluster(hdfs, alias));
        
        return true;
	}

}
