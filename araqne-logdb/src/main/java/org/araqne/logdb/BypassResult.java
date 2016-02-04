package org.araqne.logdb;

import java.io.IOException;
import java.util.Date;
import java.util.Set;

public class BypassResult implements QueryResult {

	private QueryCommand cmd;
	private QueryCommandPipe pipe;

	public BypassResult(QueryCommand cmd) {
		this.cmd = cmd;
		this.pipe = new QueryCommandPipe(cmd);
	}

	@Override
	public boolean isThreadSafe() {
		return cmd instanceof ThreadSafe;
	}

	@Override
	public void onRow(Row row) {
		pipe.onRow(row);
	}

	@Override
	public void onRowBatch(RowBatch rowBatch) {
		pipe.onRowBatch(rowBatch);
	}

	@Override
	public void onVectorizedRowBatch(VectorizedRowBatch vbatch) {
		pipe.onVectorizedRowBatch(vbatch);
	}

	@Override
	public Date getEofDate() {
		return null;
	}

	@Override
	public long getCount() {
		return 0;
	}

	@Override
	public void syncWriter() throws IOException {
	}

	@Override
	public void closeWriter() {
	}

	@Override
	public void purge() {
	}

	@Override
	public boolean isStreaming() {
		return false;
	}

	@Override
	public void setStreaming(boolean streaming) {
	}

	@Override
	public QueryResultSet getResultSet() throws IOException {
		return null;
	}

	@Override
	public Set<QueryResultCallback> getResultCallbacks() {
		return null;
	}

	@Override
	public void openWriter() throws IOException {
	}
}
