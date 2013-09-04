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
package org.araqne.logdb.query.parser;

import org.araqne.logdb.LogQueryCommand;
import org.araqne.logdb.LogQueryCommandParser;
import org.araqne.logdb.LogQueryContext;
import org.araqne.logdb.LogQueryParseException;
import org.araqne.logdb.query.command.Import;
import org.araqne.logstorage.LogStorage;

/**
 * @since 1.6.6
 * @author xeraph
 * 
 */
public class ImportParser implements LogQueryCommandParser {

	private LogStorage storage;

	public ImportParser(LogStorage storage) {
		this.storage = storage;
	}

	@Override
	public String getCommandName() {
		return "import";
	}

	@Override
	public LogQueryCommand parse(LogQueryContext context, String commandString) {
		if (context == null || !context.getSession().isAdmin())
			throw new LogQueryParseException("no-permission", -1);

		String tableName = commandString.substring(getCommandName().length()).trim();

		return new Import(storage, tableName);
	}
}
