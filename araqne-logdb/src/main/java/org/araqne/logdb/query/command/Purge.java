/**
 * Copyright 2014 Eediom Inc.
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
package org.araqne.logdb.query.command;

import java.util.Date;
import java.util.List;

import org.araqne.logdb.QueryCommand;
import org.araqne.logstorage.LogStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 2.2.10
 * 
 * @author darkluster
 * 
 */
public class Purge extends QueryCommand {
	private final Logger logger = LoggerFactory.getLogger(Purge.class);
	private LogStorage storage;

	private List<String> tableNames;
	private Date from;
	private Date to;

	public Purge(LogStorage storage, List<String> tableNames, Date from, Date to) {
		this.storage = storage;
		this.tableNames = tableNames;
		this.from = from;
		this.to = to;
	}

	@Override
	public String getName() {
		return "purge";
	}

	@Override
	public void onStart() {
		for (String tableName : this.tableNames)
			storage.purge(tableName, this.from, this.to);
	}
}
