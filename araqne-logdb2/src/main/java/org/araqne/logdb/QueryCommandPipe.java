package org.araqne.logdb;

public class QueryCommandPipe implements RowPipe {
	private QueryCommand dst;

	public QueryCommandPipe(QueryCommand dst) {
		this.dst = dst;
	}

	@Override
	public void onRow(Row row) {
		dst.onPush(row);
	}
}
