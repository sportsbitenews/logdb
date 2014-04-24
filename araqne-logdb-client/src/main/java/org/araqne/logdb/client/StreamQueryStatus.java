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
package org.araqne.logdb.client;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 스트림 쿼리 상태를 표현합니다.
 * 
 * @since 0.9.5
 * @author xeraph@eediom.com
 * 
 */
public class StreamQueryStatus {

	private StreamQueryInfo query;
	private long inputCount;
	private Date lastRefresh;
	private boolean running;

	public StreamQueryInfo getStreamQuery() {
		return query;
	}

	public void setStreamQuery(StreamQueryInfo query) {
		this.query = query;
	}

	public long getInputCount() {
		return inputCount;
	}

	public void setInputCount(long inputCount) {
		this.inputCount = inputCount;
	}

	public Date getLastRefresh() {
		return lastRefresh;
	}

	public void setLastRefresh(Date lastRefresh) {
		this.lastRefresh = lastRefresh;
	}

	public boolean isRunning() {
		return running;
	}

	public void setRunning(boolean running) {
		this.running = running;
	}

	@Override
	public String toString() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return "name=" + query.getName() + ", input=" + inputCount + ", last refresh=" + df.format(lastRefresh) + ", running="
				+ running;
	}

}
