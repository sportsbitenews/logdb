package org.araqne.logdb;

public class SubQueryTask extends QueryTask {

	private Query subQuery;

	public SubQueryTask(Query subQuery) {
		this.subQuery = subQuery;
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
