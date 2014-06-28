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
package org.araqne.logdb;

import java.io.IOException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.araqne.logdb.QueryCommand.Status;
import org.araqne.logdb.query.engine.QueryTaskScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultQuery implements Query {
	private Logger logger = LoggerFactory.getLogger(DefaultQuery.class);
	private static AtomicInteger nextId = new AtomicInteger(1);

	private final int id = nextId.getAndIncrement();
	private QueryContext context;
	private String queryString;
	private List<QueryCommand> commands;
	private Date lastStarted;
	private QueryResult result;

	private QueryStopReason stopReason;
	private Throwable cause;

	private RunMode runMode = RunMode.FOREGROUND;
	private QueryCallbacks callbacks = new QueryCallbacks();

	// task scheduler which consider dependency
	private QueryTaskScheduler scheduler;

	private AtomicLong stamp = new AtomicLong(1);

	public DefaultQuery(QueryContext context, String queryString, List<QueryCommand> commands, QueryResultFactory resultFactory) {
		this.context = context;
		this.queryString = queryString;
		this.commands = commands;
		this.scheduler = new QueryTaskScheduler(this, commands);

		for (QueryCommand cmd : commands)
			cmd.setQuery(this);

		if (resultFactory != null)
			openResult(resultFactory);

		if (context != null)
			context.getQueries().add(this);
	}

	private void openResult(QueryResultFactory resultFactory) {
		try {
			QueryResultConfig config = new QueryResultConfig();
			config.setQuery(this);
			result = resultFactory.createResult(config);
		} catch (IOException e) {
			result.closeWriter();
			result.purge();
			throw new IllegalStateException("cannot create result, maybe disk full", e);
		}
	}

	public void preRun() {
		// connect all pipe
		QueryCommand last = null;
		for (QueryCommand cmd : commands) {
			if (last != null)
				last.setOutput(new QueryCommandPipe(cmd));
			last = cmd;
		}

		commands.get(commands.size() - 1).setOutput(result);
		logger.trace("araqne logdb: run query => {}", queryString);
		for (QueryCommand command : commands)
			command.onStart();
	}

	@Override
	public void run() {
		try {
			lastStarted = new Date();
			if (commands.isEmpty())
				return;

			preRun();

			scheduler.run();
		} catch (Throwable t) {
			logger.error("araqne logdb: query failed - " + this, t);
		}
	}

	@Override
	public void postRun() {
		stop(QueryStopReason.End);
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
	public long getStartTime() {
		return scheduler.getStartTime();
	}

	@Override
	public boolean isFinished() {
		return scheduler.isFinished();
	}

	@Override
	public long getFinishTime() {
		return scheduler.getFinishTime();
	}

	@Override
	public void purge() {
		// prevent deleted result file access caused by result check of query
		// callback or timeline callbacks
		callbacks.getStatusCallbacks().clear();

		if (result != null)
			result.purge();
	}

	@Override
	public QueryStopReason getStopReason() {
		return stopReason;
	}

	@Override
	public Throwable getCause() {
		return cause;
	}

	@Override
	public void stop(QueryStopReason reason) {
		if (stopReason != null)
			return;

		stopReason = reason;

		// stop tasks
		scheduler.stop(reason);

		// send eof and close result writer
		for (QueryCommand cmd : commands) {
			if (cmd.getStatus() == Status.Finalizing || cmd.getStatus() == Status.End)
				continue;

			cmd.setStatus(Status.Finalizing);
			try {
				cmd.onClose(reason);
			} catch (Throwable t) {
				logger.error("araqne logdb: cannot close command " + cmd.getName(), t);
			}
			cmd.setStatus(Status.End);
		}

		try {
			if (result != null)
				result.closeWriter();
		} catch (Throwable t) {
			logger.error("araqne logdb: cannot close query result", t);
		}
	}

	@Override
	public void stop(Throwable cause) {
		if (stopReason != null)
			return;

		this.cause = cause;
		stop(QueryStopReason.CommandFailure);
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
		if (result == null)
			return null;

		try {
			result.syncWriter();
		} catch (Throwable t) {
			logger.debug("araqne logdb: result disk sync failed", t);
		}

		return result.getResultSet();
	}

	@Override
	public Long getResultCount() throws IOException {
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
	public long getNextStamp() {
		return stamp.incrementAndGet();
	}

	@Override
	public String toString() {
		return "id=" + id + ", query=" + queryString;
	}
}
