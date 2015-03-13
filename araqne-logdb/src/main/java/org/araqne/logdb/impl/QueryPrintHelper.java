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
import java.util.List;

import org.araqne.api.ScriptContext;
import org.araqne.logdb.Query;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.RunMode;

/**
 * @since 0.14.0
 * @author xeraph
 */
public class QueryPrintHelper {
	public static void printQueries(ScriptContext context, Collection<Query> qs, String queryFilter) {
		String header = "Log Queries";
		if (queryFilter != null)
			header += " (filter by \"" + queryFilter + "\")";

		context.println(header);
		int lineLen = header.length() + 2;
		for (int i = 0; i < lineLen; i++)
			context.print("-");

		context.println("");

		ArrayList<Query> queries = new ArrayList<Query>(qs);
		Collections.sort(queries, new Comparator<Query>() {
			@Override
			public int compare(Query o1, Query o2) {
				return o1.getId() - o2.getId();
			}
		});

		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		for (Query q : queries) {
			if (queryFilter != null && !q.getQueryString().contains(queryFilter))
				continue;

			String when = "";
			if (q.isStarted())
				when = " at " + df.format(new Date(q.getStartTime()));
			String loginName = "no session";
			if (q.getContext() != null && q.getContext().getSession() != null)
				loginName = q.getContext().getSession().getLoginName();

			Long count = null;
			try {
				count = q.getResultCount();
				if (count == null)
					count = 0L;
			} catch (Throwable t) {
			}

			String queryString = q.getQueryString();
			if (queryString.length() > 60)
				queryString = queryString.substring(0, 60) + "...";

			String runMode = q.getRunMode() == RunMode.BACKGROUND ? " (bg)" : "";
			String lastStatus = q.getCommands().get(q.getCommands().size() - 1).getStatus().toString();
			context.println(String.format("[%d:%s:%s%s%s] %s => %d", q.getId(), lastStatus, loginName, when, runMode,
					queryString, count));
		}
	}

	public static String getQueryStatus(Query query) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		String when = " \t/ not started yet";
		if (query.isStarted()) {
			long sec = System.currentTimeMillis() - query.getStartTime();
			when = String.format(" \t/ %s, %d seconds ago", sdf.format(new Date(query.getStartTime())), sec / 1000);
		}

		String loginName = query.getContext().getSession().getLoginName();
		String status = String.format("[%d:%s] %s%s\n", query.getId(), loginName, query.getQueryString(), when);

		int i = 0;
		for (Query q : query.getContext().getQueries()) {
			if (i++ != 0)
				status += "Sub Query #" + q.getId() + "\n";

			status += getCommandStatuses(q.getCommands(), 2);
		}

		return status;
	}

	private static String getCommandStatuses(List<QueryCommand> commands, int indent) {
		String status = "";
		for (QueryCommand cmd : commands) {
			if (cmd.getNestedCommands().size() > 0) {
				status += tab(indent) + "Command [" + cmd.getName() + "]\n";
				status += getCommandStatuses(cmd.getNestedCommands(), indent + 2);
			} else {
				String taskId = cmd.getMainTask() != null ? 
						String.format("%d: ", cmd.getMainTask().getID()) : "";
				status += String.format(tab(indent) + "[%s] %s%s \t/ passed %d data to next query\n", cmd.getStatus(),
						taskId, cmd.toString(), cmd.getOutputCount());
			}
		}
		return status;
	}

	private static String tab(int n) {
		StringBuilder sb = new StringBuilder(100);
		for (int i = 0; i < n; i++)
			sb.append(" ");
		return sb.toString();
	}
}
