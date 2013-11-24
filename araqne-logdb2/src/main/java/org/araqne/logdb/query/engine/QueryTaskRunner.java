package org.araqne.logdb.query.engine;

import java.util.concurrent.atomic.AtomicLong;

import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.query.engine.QueryTask.TaskStatus;
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

			QueryTaskEvent completeEvent = new QueryTaskEvent(task);
			triggerCompleteEvent(task, completeEvent);
		} catch (Throwable t) {
			task.setStatus(TaskStatus.CANCELED);
			task.setFailure(t);
			scheduler.stop(QueryStopReason.CommandFailure);
		} finally {
			task.onCleanUp();
		}
	}

	private void triggerStartEvent(QueryTask task, QueryTaskEvent event) {
		for (QueryTaskListener listener : task.getListeners())
			listener.onStart(event);

		// bubble
		if (task.getParentTask() != null)
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

}
