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
package org.araqne.logdb.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class HdfsHelper {
	private HdfsHelper() {
	}

	public static FileSystem getFileSystem(HdfsSiteManager siteManager, String name) throws IOException {
		HdfsSite site = siteManager.getSite(name);
		if (site == null)
			return null;

		return getFileSystem(site.getFileSystemUri());
	}

	public static FileSystem getFileSystem(String fsUri) throws IOException {
		Configuration config = new Configuration();
		config.setClassLoader(Configuration.class.getClassLoader());
		config.set("fs.default.name", fsUri);
		return FileSystem.get(config);
	}
}
