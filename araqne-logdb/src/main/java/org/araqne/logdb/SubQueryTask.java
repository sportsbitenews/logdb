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
	public void run() {
		subQuery.run();
		subQuery.awaitFinish();
	}

	@Override
	public synchronized void setStatus(TaskStatus status) {
		if (status == TaskStatus.CANCELED)
			subQuery.cancel(QueryStopReason.Interrupted);
		
		super.setStatus(status);
	}
	
	
}
