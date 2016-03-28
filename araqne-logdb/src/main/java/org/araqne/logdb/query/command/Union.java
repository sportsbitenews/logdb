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

import java.util.List;

import org.araqne.logdb.BypassResultFactory;
import org.araqne.logdb.DefaultQuery;
import org.araqne.logdb.Query;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryTask;
import org.araqne.logdb.QueryThreadPoolService;
import org.araqne.logdb.SubQueryCommand;
import org.araqne.logdb.SubQueryTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 2.2.13
 * @author xeraph
 */
public class Union extends QueryCommand implements SubQueryCommand {
	private final Logger slog = LoggerFactory.getLogger(Union.class);

	private String subQueryString;
	private Query subQuery;
	private SubQueryTask subQueryTask;

	public Union(QueryContext context, List<QueryCommand> commands, QueryThreadPoolService queryThreadPool) {
		this.subQueryString = buildQueryString(commands);
		this.subQuery = new DefaultQuery(context, subQueryString, commands, new BypassResultFactory(this), queryThreadPool);
		this.subQueryTask = new SubQueryTask(subQuery);
	}

	@Override
	public boolean isDriver() {
		return true;
	}

	@Override
	public QueryTask getMainTask() {
		return subQueryTask;
	}

	@Override
	public Query getSubQuery() {
		return subQuery;
	}

	@Override
	public String getName() {
		return "union";
	}

	@Override
	public String toString() {
		return "union [ " + subQuery.getQueryString() + " ]";
	}

	public static String buildQueryString(List<QueryCommand> commands) {
		StringBuilder sb = new StringBuilder();
		int i = 0;
		for (QueryCommand cmd : commands) {
			if (i++ != 0)
				sb.append(" | ");
			sb.append(cmd.toString());
		}
		return sb.toString();
	}
}