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
package org.araqne.logdb;

import java.util.List;
import java.util.Locale;
import java.util.Map;

public interface QueryParserService {
	/**
	 * 
	 * @since 2.6.24
	 */
	List<QueryCommandParser> getCommandParsers();

	/**
	 * @since 2.0.3
	 */
	QueryCommandParser getCommandParser(String name);

	List<QueryCommand> parseCommands(QueryContext context, String queryString);

	String formatErrorMessage(String errorCode, Locale locale, Map<String, String> params);

	void addCommandParser(QueryCommandParser parser);

	void removeCommandParser(QueryCommandParser parser);

	FunctionRegistry getFunctionRegistry();

	Map<String, QueryErrorMessage> getErrorMessages();
}
