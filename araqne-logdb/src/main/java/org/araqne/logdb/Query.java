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
import java.util.List;
import java.util.Map;

public interface Query extends Runnable {
	boolean isAccessible(Session session);

	QueryContext getContext();

	int getId();

	String getQueryString();

	boolean isStarted();

	boolean isFinished();

	boolean isCancelled();

	void preRun();

	void postRun();

	QueryStopReason getStopReason();

	/**
	 * @since 2.2.17
	 * @return the cause exception of query failure
	 */
	Throwable getCause();

	/**
	 * ensure query is stopped after return
	 */
	void stop();

	/**
	 * signal query stop 
	 */
	void cancel(QueryStopReason reason);

	void cancel(Throwable cause);

	@Deprecated
	void stop(QueryStopReason reason);

	@Deprecated
	void stop(Throwable cause);

	void purge();

	long getStartTime();

	long getFinishTime();

	@Deprecated
	Date getLastStarted();

	/**
	 * elapsed time between start and eof
	 * 
	 * @return the elapsed time in milliseconds, or return null if query is not
	 *         started yet
	 */
	@Deprecated
	Long getElapsedTime();

	/**
	 * @return current loaded result count or null if query is not started
	 * @throws IOException
	 */
	Long getResultCount() throws IOException;

	QueryResult getResult();

	QueryResultSet getResultSet() throws IOException;

	List<Map<String, Object>> getResultAsList() throws IOException;

	List<Map<String, Object>> getResultAsList(long offset, int limit) throws IOException;

	List<QueryCommand> getCommands();

	QueryCallbacks getCallbacks();

	/**
	 * If run mode is background, query will not be removed at logout
	 * 
	 * @since 0.17.0
	 */
	RunMode getRunMode();

	/**
	 * set query run mode
	 * 
	 * @since 0.17.0
	 */
	void setRunMode(RunMode mode, QueryContext context);

	/**
	 * return stamp number to client for message ordering
	 */
	long getNextStamp();

	/**
	 * @return the field order only if 'fields' or 'proc' command is used,
	 *         otherwise null
	 * @since 2.4.60
	 */
	List<String> getFieldOrder();
	
	void awaitFinish();
}
