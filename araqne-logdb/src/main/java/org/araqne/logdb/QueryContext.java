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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class QueryContext {
	private Session session;
	private QueryContext parent;
	private ParserContext parserContext = new ParserContext();
	private Map<String, Object> constants = Collections.synchronizedMap(new HashMap<String, Object>());

	/**
	 * includes main and dynamic sub queries
	 */
	private List<Query> queries = new CopyOnWriteArrayList<Query>();

	public QueryContext(Session session) {
		this.session = session;
	}

	public QueryContext(Session session, QueryContext parent) {
		this(session);
		this.parent = parent;
		this.constants.putAll((parent.getConstants()));
	}

	public Query getMainQuery() {
		if (parent != null)
			return parent.getMainQuery();

		return this.getQueries().get(0);
	}

	public void setSession(Session session) {
		this.session = session;
	}

	public Session getSession() {
		return session;
	}

	public ParserContext getParserContext() {
		return parserContext;
	}

	public Map<String, Object> getConstants() {
		return constants;
	}

	public List<Query> getQueries() {
		return queries;
	}
}
