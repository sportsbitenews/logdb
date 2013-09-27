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
package org.araqne.logdb.query.command;

import java.util.Date;
import java.util.Map;

import org.araqne.log.api.LogParser;
import org.araqne.log.api.LogParserInput;
import org.araqne.log.api.LogParserOutput;
import org.araqne.logdb.LogMap;
import org.araqne.logdb.LogQueryCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 1.6.6
 * @author xeraph
 * 
 */
public class Parse extends LogQueryCommand {
	private final Logger logger = LoggerFactory.getLogger(Parse.class);
	private final int parserVersion;
	private final LogParserInput input = new LogParserInput();
	private LogParser parser;

	public Parse(LogParser parser) {
		this.parser = parser;
		this.parserVersion = parser.getVersion();
	}

	@Override
	public void push(LogMap m) {
		try {
			if (parserVersion == 2) {
				Object table = m.get("_table");
				Object time = m.get("_time");

				if (time != null && time instanceof Date)
					input.setDate((Date) time);
				else
					input.setDate(null);

				if (table != null && table instanceof String)
					input.setSource((String) table);
				else
					input.setSource(null);

				input.setData(m.map());

				LogParserOutput output = parser.parse(input);
				if (output != null) {
					for (Map<String, Object> row : output.getRows()) {
						if (m.get("_id") != null && !row.containsKey("_id"))
							row.put("_id", m.get("_id"));
						if (time != null && !row.containsKey("_time"))
							row.put("_time", m.get("_time"));
						if (table != null && !row.containsKey("_table"))
							row.put("_table", m.get("_table"));

						write(new LogMap(row));
					}
				}
			} else {
				Map<String, Object> row = parser.parse(m.map());
				if (row != null) {
					if (!row.containsKey("_id"))
						row.put("_id", m.get("_id"));
					if (!row.containsKey("_time"))
						row.put("_time", m.get("_time"));
					if (!row.containsKey("_table"))
						row.put("_table", m.get("_table"));

					write(new LogMap(row));
				}
			}
		} catch (Throwable t) {
			if (logger.isDebugEnabled())
				logger.debug("araqne logdb: cannot parse " + m.map() + ", query - " + getQueryString(), t);
		}
	}

	@Override
	public boolean isReducer() {
		return false;
	}
}
