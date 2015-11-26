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
package org.araqne.logdb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * physical plan
 * 
 * @author xeraph
 * 
 */
public abstract class QueryTask implements Runnable {
	private final Logger logger = LoggerFactory.getLogger(QueryTask.class);

	public enum TaskStatus {
		INIT, RUNNING, FINALIZING, COMPLETED, CANCELED
	}

	private CountDownLatch latch = new CountDownLatch(1);

	private TaskStatus status = TaskStatus.INIT;
	private Throwable failure;
	private QueryTask parentTask;
	private CopyOnWriteArraySet<QueryTask> subTasks = new CopyOnWriteArraySet<QueryTask>();
	private CopyOnWriteArraySet<QueryTask> dependencies = new CopyOnWriteArraySet<QueryTask>();
	private CopyOnWriteArraySet<QueryTaskListener> listeners = new CopyOnWriteArraySet<QueryTaskListener>();

	private static AtomicLong l = new AtomicLong(1);

	private long id = l.incrementAndGet();

	public synchronized TaskStatus getStatus() {
		return status;
	}

	public synchronized CountDownLatch getLatch() {
		return this.latch;
	}

	public synchronized void done() {
		latch.countDown();
	}

	public synchronized void setStatus(TaskStatus status) {
		this.status = status;
	}

	public synchronized Throwable getFailure() {
		return failure;
	}

	public synchronized void setFailure(Throwable failure) {
		this.failure = failure;
	}

	public synchronized Collection<QueryTask> getSubTasks() {
		return new ArrayList<QueryTask>(subTasks);
	}

	public synchronized QueryTask getParentTask() {
		return parentTask;
	}

	public synchronized void setParentTask(QueryTask parentTask) {
		this.parentTask = parentTask;
	}

	public synchronized void addSubTask(QueryTask task) {
		if (task == null)
			throw new IllegalArgumentException("null task is not allowed");
		task.setParentTask(this);
		subTasks.add(task);
	}

	public synchronized void removeSubTask(QueryTask task) {
		if (task == null)
			throw new IllegalArgumentException("null task is not allowed");
		subTasks.remove(task);
	}

	public synchronized boolean isStopped() {
		return (this.status == TaskStatus.CANCELED || this.status == TaskStatus.COMPLETED);
	}

	public synchronized boolean isRunnable() {
		for (QueryTask t : dependencies)
			if (!isStopped())
				return false;

		return status == TaskStatus.INIT;
	}

	// invoked when task is started
	public synchronized void onStart() {
	}

	// like finally block, regardless of normal complete or cancel
	public synchronized void onCleanUp() {
	}

	public synchronized Collection<QueryTask> getDependencies() {
		return dependencies;
	}

	public synchronized void addDependency(QueryTask task) {
		checkNotNull(task, "task");

		if (logger.isDebugEnabled())
			logger.debug("araqne logdb: [{}] depends on [{}] task", this, task);

		dependencies.add(task);
	}

	public synchronized void removeDependency(QueryTask task) {
		checkNotNull(task, "task");
		dependencies.remove(task);
	}

	public synchronized Collection<QueryTaskListener> getListeners() {
		return listeners;
	}

	public synchronized void addListener(QueryTaskListener listener) {
		checkNotNull(listener, "listener");
		listeners.add(listener);
	}

	public synchronized void removeListener(QueryTaskListener listener) {
		checkNotNull(listener, "listener");
		listeners.remove(listener);
	}

	private void checkNotNull(Object o, String name) {
		if (o == null)
			throw new IllegalArgumentException("null " + name + " is not allowed");
	}

	public synchronized long getID() {
		return id;
	}
}
