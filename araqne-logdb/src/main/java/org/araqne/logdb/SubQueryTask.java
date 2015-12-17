package org.araqne.logdb;

public class SubQueryTask extends QueryTask {

	private Query subQuery;
	private QueryContext mainContext;

	public SubQueryTask(Query subQuery) {
		this.subQuery = subQuery;
	}

	public SubQueryTask(Query subQuery, QueryContext mainContext) {
		// procedure should not expose sub query, however sub query error should
		// be propagated.
		this.subQuery = subQuery;
		this.mainContext = mainContext;
	}

	public Query getSubQuery() {
		return subQuery;
	}

	@Override
	public boolean isRunnable() {
		if (!subQuery.isRunnable())
			return false;

		return super.isRunnable();
	}

	@Override
	public void run() {
		subQuery.run();
		subQuery.awaitFinish();

		if (subQuery.isCancelled()) {
			if (mainContext != null)
				mainContext.getMainQuery().cancel(subQuery.getCause());
			else
				subQuery.getContext().getMainQuery().cancel(subQuery.getCause());

		}
	}

	@Override
	public synchronized void setStatus(TaskStatus status) {
		if (status == TaskStatus.CANCELED)
			subQuery.cancel(QueryStopReason.Interrupted);

		super.setStatus(status);
	}

	@Override
	public String toString() {
		return "subquery task: " + subQuery.toString();
	}
}
