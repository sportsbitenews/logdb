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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.araqne.logdb.LogMap;
import org.araqne.logdb.LogQueryCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsCommand extends LogQueryCommand {
	private final Logger logger = LoggerFactory.getLogger(HdfsCommand.class);
	private HdfsSite site;
	private String path;

	public HdfsCommand(HdfsSite site, String path) {
		this.site = site;
		this.path = path;
	}

	@Override
	public void start() {
		BufferedReader br = null;
		try {
			status = Status.Running;

			FileSystem dfs = HdfsHelper.getFileSystem(site.getFileSystemUri());
			FSDataInputStream is = dfs.open(new Path(path));
			br = new BufferedReader(new InputStreamReader(is));

			while (true) {
				String line = br.readLine();
				if (line == null)
					break;

				Map<String, Object> m = new HashMap<String, Object>();
				m.put("line", line);

				write(new LogMap(m));
			}

		} catch (Throwable t) {
			logger.error("araqne logdb hdfs: hdfs error", t);
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
				}
			}
		}
		eof(false);
	}

	@Override
	public void push(LogMap m) {
	}

	@Override
	public boolean isReducer() {
		return false;
	}
}
