/**
 * Copyright 2014 Eediom Inc.
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

import java.io.IOException;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.slf4j.Logger;

public class StreamResult implements QueryResult {
	private final Logger slog = org.slf4j.LoggerFactory.getLogger(StreamResult.class);
	private CopyOnWriteArraySet<RowPipe> pipes;
	private boolean streaming;

	public StreamResult(CopyOnWriteArraySet<RowPipe> pipes) {
		this.pipes = pipes;
	}

	@Override
	public boolean isThreadSafe() {
		return true;
	}

	@Override
	public void onRow(Row row) {
		for (RowPipe pipe : pipes) {
			try {
				pipe.onRow(row);
			} catch (Throwable t) {
				slog.error("araqne logdb: cannot pass stream result", t);
			}
		}
	}

	@Override
	public void onRowBatch(RowBatch rowBatch) {
		for (RowPipe pipe : pipes) {
			try {
				pipe.onRowBatch(rowBatch);
			} catch (Throwable t) {
				slog.error("araqne logdb: cannot pass stream result", t);
			}
		}
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
		return streaming;
	}

	@Override
	public void setStreaming(boolean streaming) {
		this.streaming = streaming;
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
