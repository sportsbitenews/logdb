package org.araqne.storage.hdfs.script;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.araqne.api.Script;
import org.araqne.api.ScriptContext;
import org.araqne.api.ScriptUsage;
import org.araqne.api.ScriptArgument;
import org.araqne.storage.hdfs.HDFSCluster;
import org.araqne.storage.hdfs.HDFSStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HDFSStorageScript implements Script {
	private final Logger logger = LoggerFactory.getLogger("org.araqne.storage.hdfs.script.HDFSStorageScript");
	private ScriptContext context;
	private HDFSStorageManager hdfsStorageManager;
	

	public HDFSStorageScript(HDFSStorageManager hdfsStorageManager) {
		this.hdfsStorageManager = hdfsStorageManager;
	}

	@Override
	public void setScriptContext(ScriptContext context) {
		this.context = context;
	}
	
	@ScriptUsage(description = "print hdfs storage clusters", arguments = {})
	public void HDFSStorageClusters(String[] args) {
		context.println("HDFS Storage Status");
		context.println("-------------------");
		for (HDFSCluster cluster : hdfsStorageManager.getClusters()) {
			context.print("protocol: ");
			context.print(cluster.getProtocol());
			context.print("\talias: ");
			context.print(cluster.getAlias());
			context.print("\tFilesystem: ");
			context.print(cluster.getFileSystem().getCanonicalServiceName());
			context.print("\t");
			context.print(cluster.getFileSystem().getUri().getHost());
			context.print(":");
			context.print(cluster.getFileSystem().getUri().getPort());
			context.println();
		}
	}
	
	@ScriptUsage(description = "add hdfs storage clusters", arguments = { 
			@ScriptArgument(name = "name", type = "string", description = "cluster name")})
	public void addHDFSStorageCluster(String[] args) {
		try {
			List<String> confFiles = new ArrayList<String>();
			while (true) {
				String line = readLine("input conf file name (to finish input, press enter)");
				if (line == null || line.trim().length() == 0)
					break;
				
				confFiles.add(line.trim());
			}
			
			hdfsStorageManager.addCluster(confFiles, args[0]);
			logger.info("araqne hdfs storage: added hdfs cluster " + args[0]);
		} catch (Throwable t) {
			context.println(t.getMessage());
			logger.error("araqne hdfs storage: cannot add hdfs cluster " + args[0], t);
		}
	}
	
	@ScriptUsage(description = "remove hdfs storage clusters", arguments = { 
			@ScriptArgument(name = "name", type = "string", description = "cluster name")})
	public void removeHDFSStorageCluster(String[] args) {
		if (hdfsStorageManager.removeCluster(args[0]))
			context.println("removed cluster " + args[0]);
		logger.info("araqne hdfs storage: removed hdfs cluster " + args[0]);
	}
	

	private String readLine(String question) throws InterruptedException {
		context.print(question);
		String line = context.readLine();
		if (line.trim().isEmpty())
			return null;

		return line;
	}
}
