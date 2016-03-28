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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.araqne.logdb.Query;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryCommand.Status;
import org.araqne.logdb.QueryStatusCallback;
import org.araqne.logdb.QueryTask;
import org.araqne.logdb.QueryTask.TaskStatus;
import org.araqne.logdb.QueryTaskEvent;
import org.araqne.logdb.QueryTaskListener;
import org.araqne.logdb.QueryThreadPoolService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryTaskScheduler implements Runnable {
	private final Logger logger = LoggerFactory.getLogger(QueryTaskScheduler.class);

	private boolean started;

	// regardless of canceled or not, just finished
	private boolean finished;

	private long startTime;
	private long finishTime;

	private Query query;
	private QueryThreadPoolService threadPool;

	// logical query command pipe
	private List<QueryCommand> pipeline = new ArrayList<QueryCommand>();

	// monitor task complete and start new ready tasks
	private QueryTaskTracer tracer = new QueryTaskTracer();

	private ReadWriteLock rwLock = new ReentrantReadWriteLock(true);

	public QueryTaskScheduler(Query query, List<QueryCommand> pipeline, QueryThreadPoolService threadPool) {
		this.query = query;
		this.pipeline = pipeline;
		this.threadPool = threadPool;
	}

	public Query getQuery() {
		return query;
	}

	public boolean isStarted() {
		return started;
	}

	public long getStartTime() {
		return startTime;
	}

	public boolean isFinished() {
		return finished;
	}

	public long getFinishTime() {
		return finishTime;
	}

	@Override
	public void run() {
		started = true;
		startTime = System.currentTimeMillis();

		for (QueryCommand cmd : pipeline) {
			cmd.setStatus(Status.Running);

			QueryTask mainTask = cmd.getMainTask();
			if (mainTask != null) {
				tracer.addDependency(mainTask);
				mainTask.addListener(tracer);
			}
		}

		startReadyTasks();
	}

	public void cancel() {
		// query can be cancelled at preRun(), tracer don't have any link.
		// tracer itself should be started even if query is cancelled.
		for (QueryCommand cmd : pipeline) {
			QueryTask mainTask = cmd.getMainTask();
			if (mainTask != null) {
				markCancelRecursively(mainTask);
			}
		}
	}

	public void awaitTaskDoneRecusively(QueryTask task) throws InterruptedException {
		for (QueryTask subTask : task.getSubTasks()) {
			awaitTaskDoneRecusively(subTask);
		}

		task.await();
	}

	public void stop() {
		Lock lock = rwLock.writeLock();
		try {
			lock.lock();

			if (!tracer.isStopped()) {
				for (QueryTask dependency : tracer.getDependencies()) {
					markCancelRecursively(dependency);
				}

				for (QueryTask dependency : tracer.getDependencies()) {
					try {
						awaitTaskDoneRecusively(dependency);
					} catch (InterruptedException e) {
						logger.error("QueryTaskScheduler.stop() failed because waiting runnigTask failed", e);
					}
				}

			}

		} finally {
			lock.unlock();
		}

		startRecursively(tracer);
	}

	private synchronized void startReadyTasks() {
		// later task runner can be completed before tracer.run(), and can cause
		// duplicated query finish callback
		boolean finished = tracer.isRunnable() || tracer.getStatus() == TaskStatus.CANCELED;

		for (QueryCommand cmd : pipeline) {
			QueryTask mainTask = cmd.getMainTask();
			if (mainTask != null)
				startRecursively(mainTask);
		}

		// all main task completed?
		if (finished)
			tracer.run();

	}

	private void startRecursively(QueryTask task) {
		if (task.isRunnable()) {
			// prevent duplicated run caused by late thread start
			if (logger.isDebugEnabled())
				logger.debug("araqne logdb: query [{}] task [{}:{}] start thread",
						new Object[] { query.getId(), task.getID(), task });

			task.setStatus(TaskStatus.RUNNING);
			QueryTaskRunner taskRunner = new QueryTaskRunner(this, task);
			threadPool.execute(taskRunner, taskRunner.getThreadName());
		} else {
			if (logger.isDebugEnabled()) {
				StringBuilder sb = new StringBuilder();
				for (QueryTask d : task.getDependencies())
					sb.append("\n\t" + d.getID() + ":" + d + " " + d.getStatus());

				if (logger.isDebugEnabled())
					logger.debug("araqne logdb: query [{}] task [{}:{} {}] is not runnable. dependencies => [{}]",
							new Object[] { query.getId(), task.getID(), task, task.getStatus(), sb.toString() });
			}
		}

		for (QueryTask subTask : task.getSubTasks())
			startRecursively(subTask);
	}

	private void markCancel(QueryTask task) {
		if (task.getStatus() != TaskStatus.COMPLETED) {
			task.setStatus(TaskStatus.CANCELED);
			task.done();

			if (logger.isDebugEnabled()) {
				String msg = null;
				Throwable failure = task.getFailure();
				if (failure != null)
					msg = failure.getMessage() != null ? failure.getMessage() : failure.getClass().getName();

				logger.debug("araqne logdb: canceled query [{}] task [{}] cause [{}]", new Object[] { query.getId(), task, msg });
			}
		}
	}

	private void markCancelRecursively(QueryTask task) {
		for (QueryTask subTask : task.getSubTasks()) {
			markCancelRecursively(subTask);
		}

		markCancel(task);
	}

	private class QueryTaskTracer extends QueryTask implements QueryTaskListener {

		private AtomicBoolean closed = new AtomicBoolean();

		@Override
		public void run() {
			if (!closed.compareAndSet(false, true))
				return;

			if (logger.isDebugEnabled())
				logger.debug("araqne logdb: all query [{}] tasks are completed", query.getId());

			query.postRun();
			finished = true;
			finishTime = System.currentTimeMillis();

			// notify finish immediately
			for (QueryStatusCallback c : query.getCallbacks().getStatusCallbacks()) {
				try {
					c.onChange(query);
				} catch (Throwable t) {
					logger.warn("araqne logdb: query [" + query.getId() + "] change callback should not throw any exception", t);
				}
			}
		}

		@Override
		public void onStart(QueryTaskEvent event) {
		}

		@Override
		public void onComplete(QueryTaskEvent event) {
			event.setHandled(true);

			if (logger.isDebugEnabled())
				logger.debug("araqne logdb: query [{}] task [{}:{}] completed",
						new Object[] { query.getId(), event.getTask().getID(), event.getTask() });
		}

		@Override
		public void onCleanUp(QueryTaskEvent event) {
			startReadyTasks();
		}

		@Override
		public String toString() {
			return "tracer for query " + query.getId() + ", " + query.getQueryString();
		}
	}
}
