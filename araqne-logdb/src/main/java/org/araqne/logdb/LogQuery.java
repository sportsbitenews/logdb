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
import java.util.Set;

public interface LogQuery extends Runnable {
	boolean isAccessible(Session session);

	LogQueryContext getContext();

	int getId();

	String getQueryString();

	boolean isEnd();

	boolean isCancelled();

	void cancel();

	void purge();

	Date getLastStarted();

	/**
	 * @return current loaded result count or null if query is not started
	 * @throws IOException
	 */
	Long getResultCount() throws IOException;

	LogResultSet getResult() throws IOException;

	List<Map<String, Object>> getResultAsList() throws IOException;

	List<Map<String, Object>> getResultAsList(long offset, int limit) throws IOException;

	List<LogQueryCommand> getCommands();

	Set<LogQueryCallback> getLogQueryCallback();

	void registerQueryCallback(LogQueryCallback callback);

	void unregisterQueryCallback(LogQueryCallback callback);

	void clearQueryCallbacks();

	Set<LogTimelineCallback> getTimelineCallbacks();

	void registerTimelineCallback(LogTimelineCallback callback);

	void unregisterTimelineCallback(LogTimelineCallback callback);

	void clearTimelineCallbacks();

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
	void setRunMode(RunMode mode, LogQueryContext context);
}
