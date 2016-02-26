/**
 * Copyright 2016 Eediom Inc.
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
package org.araqne.logdb.slack;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParserService;

/**
 * @author xeraph@eediom.com
 */
@Component(name = "sendslack-query-command-parser")
public class SendSlackQueryCommandParser extends AbstractQueryCommandParser {
	@Requires
	private QueryParserService parserService;

	@Override
	public String getCommandName() {
		return "sendslack";
	}

	@Validate
	public void start() {
		parserService.addCommandParser(this);
	}

	@Invalidate
	public void stop() {
		if (parserService != null)
			parserService.removeCommandParser(this);
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		try {
			String url = commandString.substring(getCommandName().length()).trim();
			return new SendSlackQueryCommand(new URL(url));
		} catch (MalformedURLException e) {
			throw new QueryParseException("invalid-slack-url", -1);
		}
	}
}
