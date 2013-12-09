package org.araqne.logdb.query.engine;

import java.util.ArrayList;
import java.util.List;

import org.araqne.logdb.Query;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.QueryCommand.Status;
import org.araqne.logdb.QueryStatusCallback;
import org.araqne.logdb.RowPipe;
import org.araqne.logdb.query.engine.QueryTask.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryTaskScheduler implements Runnable {
	private final Logger logger = LoggerFactory.getLogger(QueryTaskScheduler.class);

	private boolean started;

	// regardless of canceled or not, just finished
	private boolean finished;

	private Query query;

	// logical query command pipe
	private List<QueryCommand> pipeline = new ArrayList<QueryCommand>();

	// monitor task complete and start new ready tasks
	private QueryTaskTracer tracer = new QueryTaskTracer();

	public QueryTaskScheduler(Query query, List<QueryCommand> pipeline) {
		this.query = query;
		this.pipeline = pipeline;
	}

	public boolean isStarted() {
		return started;
	}

	public boolean isFinished() {
		return finished;
	}

	@Override
	public void run() {
		started = true;

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

	public void stop(QueryStopReason reason) {
		for (QueryCommand cmd : pipeline) {
			if (cmd.getMainTask() != null)
				stopRecursively(cmd.getMainTask());
		}

		query.stop(reason);
	}

	private synchronized void startReadyTasks() {
		// later task runner can be completed before tracer.run(), and can cause
		// duplicated query finish callback
		boolean finished = tracer.isRunnable();

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
			task.setStatus(TaskStatus.RUNNING);
			new QueryTaskRunner(this, task).start();
		} else {
			if (logger.isDebugEnabled())
				logger.debug("araqne logdb: task [{}] is not runnable", task);
		}

		for (QueryTask subTask : task.getSubTasks())
			startRecursively(subTask);
	}

	private void stopRecursively(QueryTask task) {
		if (task.getStatus() != TaskStatus.COMPLETED)
			task.setStatus(TaskStatus.CANCELED);

		for (QueryTask subTask : task.getSubTasks())
			stopRecursively(subTask);
	}

	private class QueryTaskTracer extends QueryTask implements QueryTaskListener {

		@Override
		public void run() {
			if (logger.isDebugEnabled())
				logger.debug("araqne logdb: all query [{}] task completed", query.getId());

			query.postRun();
			finished = true;

			// notify finish immediately
			for (QueryStatusCallback c : query.getCallbacks().getStatusCallbacks()) {
				c.onChange(query);
			}
		}

		@Override
		public void onStart(QueryTaskEvent event) {
		}

		@Override
		public void onComplete(QueryTaskEvent event) {
			event.setHandled(true);

			if (logger.isDebugEnabled())
				logger.debug("araqne logdb: query task [{}] completed", event.getTask());
			startReadyTasks();
		}

		@Override
		public RowPipe getOutput() {
			return null;
		}
	}

}
