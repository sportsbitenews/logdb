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
import org.araqne.confdb.Config;
import org.araqne.confdb.ConfigCollection;
import org.araqne.confdb.ConfigDatabase;
import org.araqne.confdb.ConfigIterator;
import org.araqne.confdb.ConfigService;
import org.araqne.confdb.Predicates;
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
    
    private HDFSStorageManagerImpl() {
    }
    
    @Requires
    private ConfigService confService;
    
	@Requires
	private StorageManager storageManager;
    
	@Validate
	@Override
	public void start() {
		// load from confdb
		ConfigDatabase db = confService.ensureDatabase("araqne-hdfs-storage");
		ConfigCollection col = db.ensureCollection(HDFSStorageClusterConfig.class);
		ConfigIterator it = col.findAll();
		for (HDFSStorageClusterConfig c : it.getDocuments(HDFSStorageClusterConfig.class)) {
			addCluster(c.getConfFiles(), c.getName(), false);
		}
        
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
		
		// there are a few clusters
		synchronized (this) {
			for (HDFSCluster root : clusters) {
				if (root.getProtocol().equals(protocol) && root.getAlias().equals(host)) {
					return new HDFSFilePath(root, subPath);
				}
			}
		}
		
		return null;
	}
	


	@Override
	public String[] getProtocols() {
		return protocols;
	}

	@Override
	public synchronized boolean  addCluster(List<String> confFiles, String alias) {
		return addCluster(confFiles, alias, true);
	}
	
	private boolean addCluster(List<String> confFiles, String alias, boolean saveConf) {
		Configuration conf = new Configuration();
		for (String cf : confFiles) {
			conf.addResource(new Path(cf));
		}
		
		// save configuration to confDB
		if (saveConf) {
			HDFSStorageClusterConfig c = new HDFSStorageClusterConfig(alias, confFiles);
			ConfigDatabase db = confService.ensureDatabase("araqne-hdfs-storage");
			db.add(c, "araqne-storage-hdfs", "added " + alias + " cluster");
		}
		
		return addCluster(conf, alias);
	}
	
	private boolean addCluster(Configuration conf, String alias) {
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

		// TODO: handle protocol (hftp, webhdfs, ...)
		clusters.add(new HDFSCluster(hdfs, alias));
        
        return true;
	}
	
	@Override
	public synchronized boolean removeCluster(String alias) {
		HDFSCluster cluster = null;
		for (HDFSCluster c : clusters) {
			if (c.getAlias().equals(alias)) {
				cluster = c;
				break;
			}
		}
		
		if (cluster == null)
			return false;
		
		ConfigDatabase db = confService.ensureDatabase("araqne-hdfs-storage");
		Config c = db.findOne(HDFSStorageClusterConfig.class, Predicates.field("name", cluster.getAlias()));
		if (c == null)
			throw new IllegalStateException("hdfs cluster not found, alias=" + cluster.getAlias());
		
		db.remove(c);
		
		clusters.remove(cluster);
		try {
			cluster.getFileSystem().close();
		} catch (IOException e) {
		}
		
		return true;
	}

	@Override
	public List<HDFSCluster> getClusters() {
		return clusters;
	}

}
