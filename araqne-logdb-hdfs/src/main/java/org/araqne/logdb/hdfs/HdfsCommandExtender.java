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

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logdb.LogQueryCommand;
import org.araqne.logdb.LogQueryCommandParser;
import org.araqne.logdb.LogQueryContext;
import org.araqne.logdb.LogQueryParseException;
import org.araqne.logdb.LogQueryParserService;

@Component(name = "logdb-hdfs-command-extender")
public class HdfsCommandExtender implements LogQueryCommandParser {

	@Requires
	private LogQueryParserService queryParserService;

	@Requires
	private HdfsSiteManager siteManager;

	@Validate
	public void start() {
		queryParserService.addCommandParser(this);
	}

	@Invalidate
	public void stop() {
		if (queryParserService != null)
			queryParserService.removeCommandParser(this);
	}

	@Override
	public String getCommandName() {
		return "hdfs";
	}

	@Override
	public LogQueryCommand parse(LogQueryContext context, String commandString) {
		String queryString = commandString.substring(getCommandName().length()).trim();
		String siteName = queryString.split(" ")[0].trim();

		HdfsSite site = siteManager.getSite(siteName);
		if (site == null)
			throw new LogQueryParseException("invalid-hdfs-site", -1, siteName);

		int p = queryString.indexOf(" ");
		String path = queryString.substring(p).trim();

		return new HdfsCommand(site, path);
	}

}
