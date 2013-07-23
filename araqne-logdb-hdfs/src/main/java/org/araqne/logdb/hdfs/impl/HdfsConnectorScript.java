/*
 * Copyright 2013 Eediom Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.araqne.logdb.hdfs.impl;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.araqne.api.Script;
import org.araqne.api.ScriptArgument;
import org.araqne.api.ScriptContext;
import org.araqne.api.ScriptUsage;
import org.araqne.logdb.hdfs.HdfsHelper;
import org.araqne.logdb.hdfs.HdfsSite;
import org.araqne.logdb.hdfs.HdfsSiteManager;

public class HdfsConnectorScript implements Script {
	private HdfsSiteManager siteManager;
	private ScriptContext context;

	public HdfsConnectorScript(HdfsSiteManager siteManager) {
		this.siteManager = siteManager;
	}

	@Override
	public void setScriptContext(ScriptContext context) {
		this.context = context;
	}

	public void hdfsSites(String[] args) {
		context.println("HDFS Sites");
		context.println("------------");
		for (HdfsSite site : siteManager.getSites()) {
			context.println(site);
		}
	}

	@ScriptUsage(description = "add new hdfs site", arguments = {
			@ScriptArgument(name = "site name", type = "string", description = "hdfs site name"),
			@ScriptArgument(name = "fs uri", type = "string", description = "file system uri")
	})
	public void addHdfsSite(String[] args) {
		try {
			new URI(args[1]);
		} catch (URISyntaxException e) {
			context.println("invalid file system uri: " + args[1]);
			return;
		}

		HdfsSite site = new HdfsSite();
		site.setName(args[0]);
		site.setFileSystemUri(args[1]);
		siteManager.addSite(site);

		context.println("added");
	}

	@ScriptUsage(description = "remove hdfs site", arguments = {
			@ScriptArgument(name = "site name", type = "string", description = "hdfs site name") })
	public void removeHdfsSite(String[] args) {
		siteManager.removeSite(args[0]);

		context.println("removed");
	}

	@ScriptUsage(description = "list hdfs files", arguments = {
			@ScriptArgument(name = "site name", type = "string", description = "hdfs site name"),
			@ScriptArgument(name = "path", type = "string", description = "hdfs path")
	})
	public void listHdfsFiles(String[] args) throws IOException {
		String name = args[0];
		String path = args[1];

		FileSystem fs = HdfsHelper.getFileSystem(siteManager, name);
		if (fs == null) {
			context.println("hdfs site profile not found: " + name);
			return;
		}

		context.println("Files");
		context.println("-------");
		Path p = new Path(path);
		FileStatus[] status = fs.listStatus(p);
		for (FileStatus s : status)
			context.println(s.getPath() + ": " + s.getLen());
	}
}
