/*
 * Copyright 2013 Future Systems
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

import java.util.Map;

import org.araqne.logdb.LogQueryCommand;
import org.araqne.logdb.LogQueryCommandParser;
import org.araqne.logdb.LogQueryContext;
import org.araqne.logdb.LogQueryParseException;
import org.araqne.logdb.query.command.Fulltext;
import org.araqne.logstorage.LogIndexQuery;
import org.araqne.logstorage.LogIndexer;
import org.araqne.logstorage.LogStorage;

/**
 * @since 0.9
 * @author xeraph
 */
public class FulltextParser implements LogQueryCommandParser {
	private LogStorage storage;
	private LogIndexer indexer;

	public FulltextParser(LogStorage storage, LogIndexer indexer) {
		this.storage = storage;
		this.indexer = indexer;
	}

	@Override
	public String getCommandName() {
		return "fulltext";
	}

	@Override
	public LogQueryCommand parse(LogQueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(commandString, "fulltext".length());

		@SuppressWarnings("unchecked")
		Map<String, String> option = (Map<String, String>) r.value;
		String term = commandString.substring(r.next);
		if (term.trim().isEmpty())
			throw new LogQueryParseException("term-not-found", r.next);

		LogIndexQuery indexQuery = new LogIndexQuery();
		indexQuery.setTableName(option.get("table"));
		indexQuery.setIndexName(option.get("index"));
		indexQuery.setTerm(term.trim());

		return new Fulltext(storage, indexer, indexQuery);
	}
}
