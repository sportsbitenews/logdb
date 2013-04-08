/*
 * Copyright 2013 Eediom, Inc.
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
package org.araqne.logdb.impl;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;

import org.araqne.api.ScriptContext;
import org.araqne.logdb.LogQuery;
import org.araqne.logdb.LogQueryCommand;

/**
 * @since 0.14.0
 * @author xeraph
 */
public class QueryPrintHelper {
	public static void printQueries(ScriptContext context, Collection<LogQuery> q) {
		context.println("Log Queries");
		context.println("-------------");
		ArrayList<LogQuery> queries = new ArrayList<LogQuery>(q);
		Collections.sort(queries, new Comparator<LogQuery>() {
			@Override
			public int compare(LogQuery o1, LogQuery o2) {
				return o1.getId() - o2.getId();
			}
		});

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		for (LogQuery query : queries) {
			String when = " \t/ not started yet";
			if (query.getLastStarted() != null) {
				long sec = new Date().getTime() - query.getLastStarted().getTime();
				when = String.format(" \t/ %s, %d seconds ago", sdf.format(query.getLastStarted()), sec / 1000);
			}

			String loginName = query.getContext().getSession().getLoginName();
			context.println(String.format("[%d:%s] %s%s", query.getId(), loginName, query.getQueryString(), when));

			if (query.getCommands() != null) {
				for (LogQueryCommand cmd : query.getCommands()) {
					context.println(String.format("    [%s] %s \t/ passed %d data to next query", cmd.getStatus(),
							cmd.getQueryString(), cmd.getPushCount()));
				}
			} else
				context.println("    null");
		}

	}
}
