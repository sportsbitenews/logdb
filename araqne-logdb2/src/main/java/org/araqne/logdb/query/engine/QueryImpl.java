/*
 * Copyright 2011 Future Systems
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
package org.araqne.logdb.query.engine;

import java.io.IOException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.araqne.logdb.Query;
import org.araqne.logdb.QueryCallbacks;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryResult;
import org.araqne.logdb.QueryResultSet;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.RunMode;
import org.araqne.logdb.Session;
import org.araqne.logdb.QueryCommand.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryImpl implements Query {
	private Logger logger = LoggerFactory.getLogger(QueryImpl.class);
	private static AtomicInteger nextId = new AtomicInteger(1);

	private final int id = nextId.getAndIncrement();
	private QueryContext context;
	private String queryString;
	private List<QueryCommand> commands;
	private Date lastStarted;
	private QueryResultV2 result;

	private QueryStopReason stopReason;
	private RunMode runMode = RunMode.FOREGROUND;
	private QueryCallbacks callbacks = new QueryCallbacks();

	// task scheduler which consider dependency
	private QueryTaskScheduler scheduler;

	public QueryImpl(QueryContext context, String queryString, List<QueryCommand> commands) {
		this.context = context;
		this.queryString = queryString;
		this.commands = commands;
		this.scheduler = new QueryTaskScheduler(this, commands);

		for (QueryCommand cmd : commands)
			cmd.setQuery(this);

		try {
			result = new QueryResultV2(this);
		} catch (IOException e) {
			result.closeWriter();
			result.purge();
			throw new IllegalStateException("cannot create result, maybe disk full", e);
		}
	}

	public void preRun() {
		commands.get(commands.size() - 1).setOutput(result);
		logger.trace("araqne logdb: run query => {}", queryString);
		for (QueryCommand command : commands)
			command.onStart();
	}

	@Override
	public void run() {
		lastStarted = new Date();
		if (commands.isEmpty())
			return;

		preRun();

		try {
			scheduler.run();
		} catch (Exception e) {
			logger.error("araqne logdb: query failed - " + this, e);
		}
	}

	@Override
	public void postRun() {
		// send eof and close result writer
		for (QueryCommand cmd : commands) {
			cmd.setStatus(Status.Finalizing);
			try {
				cmd.onClose(QueryStopReason.End);
			} catch (Throwable t) {
				logger.error("araqne logdb: cannot close command " + cmd.getName(), t);
			}
			cmd.setStatus(Status.End);
		}
	}

	@Override
	public boolean isAccessible(Session session) {
		Session querySession = getContext().getSession();
		if (runMode == RunMode.FOREGROUND)
			return querySession.equals(session);
		else
			return querySession.getLoginName().equals(session.getLoginName());
	}

	@Override
	public QueryContext getContext() {
		return context;
	}

	@Override
	public boolean isCancelled() {
		return stopReason != null && stopReason != QueryStopReason.End && stopReason != QueryStopReason.PartialFetch;
	}

	@Override
	public RunMode getRunMode() {
		return runMode;
	}

	@Override
	public void setRunMode(RunMode runMode, QueryContext context) {
		this.runMode = runMode;
		if (context != null)
			this.context = context;
	}

	@Override
	public int getId() {
		return id;
	}

	@Override
	public String getQueryString() {
		return queryString;
	}

	@Override
	public boolean isStarted() {
		return scheduler.isStarted();
	}

	@Override
	public boolean isFinished() {
		return scheduler.isFinished();
	}

	@Override
	public void purge() {
		// prevent deleted result file access caused by result check of query
		// callback or timeline callbacks
		callbacks.getResultCallbacks().clear();
		callbacks.getStatusCallbacks().clear();
		callbacks.getTimelineCallbacks().clear();

		if (result != null)
			result.purge();
	}

	@Override
	public void stop(QueryStopReason reason) {
		if (stopReason != null)
			return;

		stopReason = reason;

		for (QueryCommand cmd : commands)
			cmd.onClose(reason);

		result.closeWriter();
	}

	@Override
	public Date getLastStarted() {
		return lastStarted;
	}

	@Override
	public Long getElapsedTime() {
		long end = System.currentTimeMillis();
		if (result != null && result.getEofDate() != null)
			end = result.getEofDate().getTime();

		if (getLastStarted() != null)
			return end - getLastStarted().getTime();

		return null;
	}

	@Override
	public QueryResult getResult() {
		return result;
	}

	@Override
	public QueryResultSet getResultSet() throws IOException {
		return result.getResult();
	}

	@Override
	public Long getResultCount() throws IOException {
		try {
			result.syncWriter();
		} catch (Throwable t) {
			logger.debug("araqne logdb: result disk sync failed", t);
		}

		return result.getCount();
	}

	@Override
	public List<Map<String, Object>> getResultAsList() throws IOException {
		return getResultAsList(0, Integer.MAX_VALUE);
	}

	@Override
	public List<Map<String, Object>> getResultAsList(long offset, int limit) throws IOException {
		LinkedList<Map<String, Object>> l = new LinkedList<Map<String, Object>>();

		QueryResultSet rs = null;
		try {
			rs = getResultSet();
			if (rs == null)
				return null;

			long p = 0;
			long count = 0;
			while (rs.hasNext()) {
				if (count >= limit)
					break;

				Map<String, Object> m = rs.next();
				if (p++ < offset)
					continue;

				l.add(m);
				count++;
			}
		} finally {
			rs.close();
		}
		return l;
	}

	@Override
	public List<QueryCommand> getCommands() {
		return commands;
	}

	@Override
	public QueryCallbacks getCallbacks() {
		return callbacks;
	}

	@Override
	public String toString() {
		return "id=" + id + ", query=" + queryString;
	}
}
