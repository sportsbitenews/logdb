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
package org.araqne.logdb.query.engine;

import java.util.concurrent.atomic.AtomicLong;

import org.araqne.logdb.QueryCancelException;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.QueryTask;
import org.araqne.logdb.QueryTaskEvent;
import org.araqne.logdb.QueryTaskListener;
import org.araqne.logdb.QueryTask.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryTaskRunner extends Thread {
	private static AtomicLong idCounter = new AtomicLong(1);
	private final Logger logger = LoggerFactory.getLogger(QueryTaskRunner.class);
	private QueryTaskScheduler scheduler;
	private QueryTask task;

	public QueryTaskRunner(QueryTaskScheduler scheduler, QueryTask task) {
		this.scheduler = scheduler;
		this.task = task;
		setName("Query Task #" + idCounter.incrementAndGet());
	}

	@Override
	public void run() {
		try {
			logger.debug("araqne logdb: running task [{}]", task);
			QueryTaskEvent startEvent = new QueryTaskEvent(task);
			triggerStartEvent(task, startEvent);

			task.onStart();
			task.setStatus(TaskStatus.RUNNING);
			task.run();
			task.setStatus(TaskStatus.COMPLETED);

			triggerCompleteEvent(task, new QueryTaskEvent(task));
		} catch (QueryCancelException e) {
			logger.error("araqne logdb: query task [" + task + "] canceled");
			task.setStatus(TaskStatus.CANCELED);
			task.setFailure(e);
		} catch (Throwable t) {
			logger.error("araqne logdb: query task [" + task + "] failed", t);
			task.setStatus(TaskStatus.CANCELED);
			task.setFailure(t);
			scheduler.getQuery().stop(t);
		} finally {
			triggerCleanUpEvent(task, new QueryTaskEvent(task));
			task.onCleanUp();
		}
	}

	private void triggerStartEvent(QueryTask task, QueryTaskEvent event) {
		for (QueryTaskListener listener : task.getListeners())
			listener.onStart(event);

		// bubble
		if (task.getParentTask() != null)
			if (!event.isHandled())
				triggerStartEvent(task.getParentTask(), event);
	}

	private void triggerCompleteEvent(QueryTask task, QueryTaskEvent event) {
		for (QueryTaskListener listener : task.getListeners())
			listener.onComplete(event);

		// bubble
		if (task.getParentTask() != null)
			if (!event.isHandled())
				triggerCompleteEvent(task.getParentTask(), event);
	}

	private void triggerCleanUpEvent(QueryTask task, QueryTaskEvent event) {
		for (QueryTaskListener listener : task.getListeners())
			listener.onCleanUp(event);

		// bubble
		if (task.getParentTask() != null)
			if (!event.isHandled())
				triggerCleanUpEvent(task.getParentTask(), event);
	}
}
