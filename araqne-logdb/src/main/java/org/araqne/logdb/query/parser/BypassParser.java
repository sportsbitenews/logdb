/**
 * Copyright 2015 Eediom Inc.
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

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.query.command.Bypass;

public class BypassParser extends AbstractQueryCommandParser {

	public BypassParser() {
		setDescriptions("Bypass all tuples.", "모든 입력값을 그대로 출력합니다.");
	}

	@Override
	public String getCommandName() {
		return "bypass";
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		return new Bypass();
	}

}
