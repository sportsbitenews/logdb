/*
 * Copyright 2013 Eediom Inc.
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class LogQuery {
	private LogDbClient client;
	private int id;
	private String queryString;
	private String status;
	private long loadedCount;
	private boolean background;
	private Date lastStarted;
	private Long elapsed;
	private List<LogQueryCommand> commands = new ArrayList<LogQueryCommand>();
	private CopyOnWriteArrayList<WaitingCondition> waitingConditions;

	public LogQuery(LogDbClient client, int id, String queryString) {
		this.client = client;
		this.id = id;
		this.queryString = queryString;
		this.status = "Stopped";
		this.loadedCount = 0;
		this.waitingConditions = new CopyOnWriteArrayList<WaitingCondition>();
	}

	public int getId() {
		return id;
	}

	public long getLoadedCount() {
		return loadedCount;
	}

	public String getStatus() {
		return getStatus(false);
	}

	/**
	 * @since 0.8.3
	 */
	public String getStatus(boolean refresh) {
		if (refresh) {
			try {
				// comet client do not support refresh
				if (client != null)
					client.getQuery(id);
			} catch (IOException e) {
			}
		}

		return status;
	}

	public void waitUntil(Long count) {
		WaitingCondition cond = new WaitingCondition(count);
		try {
			waitingConditions.add(cond);
			synchronized (cond.signal) {
				try {
					while (!status.equals("Ended") &&
							!status.equals("Cancelled") &&
							(count == null || loadedCount < count))
						cond.signal.wait(100);
				} catch (InterruptedException e) {
				}
			}
		} finally {
			waitingConditions.remove(cond);
		}
	}

	public void updateCount(long count) {
		loadedCount = count;

		for (WaitingCondition cond : waitingConditions) {
			if (cond.threshold != null && cond.threshold <= loadedCount) {
				synchronized (cond.signal) {
					cond.signal.notifyAll();
				}
			}
		}
	}

	public void updateStatus(String status) {
		this.status = status;
		if (status.equals("Ended") || status.equals("Cancelled")) {
			for (WaitingCondition cond : waitingConditions) {
				synchronized (cond.signal) {
					cond.signal.notifyAll();
				}
			}
		}
	}

	public boolean isBackground() {
		return background;
	}

	public void setBackground(boolean background) {
		this.background = background;
	}

	public Date getLastStarted() {
		return lastStarted;
	}

	public void setLastStarted(Date lastStarted) {
		this.lastStarted = lastStarted;
	}

	public Long getElapsed() {
		return elapsed;
	}

	public void setElapsed(Long elapsed) {
		this.elapsed = elapsed;
	}

	public List<LogQueryCommand> getCommands() {
		return commands;
	}

	public void setCommands(List<LogQueryCommand> commands) {
		this.commands = commands;
	}

	private class WaitingCondition {
		private Long threshold;
		private Object signal = new Object();

		public WaitingCondition(Long threshold) {
			this.threshold = threshold;
		}
	}

	@Override
	public String toString() {
		return "id=" + id + ", query=" + queryString + ", status=" + status + ", loaded=" + loadedCount;
	}

}
