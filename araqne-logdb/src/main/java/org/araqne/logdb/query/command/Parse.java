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

import java.util.Map;

import org.araqne.log.api.LogParser;
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
	private LogParser parser;

	public Parse(LogParser parser) {
		this.parser = parser;
	}

	@Override
	public void push(LogMap m) {
		try {
			Map<String, Object> parsed = parser.parse(m.map());
			if (parsed != null)
				write(new LogMap(parsed));
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
