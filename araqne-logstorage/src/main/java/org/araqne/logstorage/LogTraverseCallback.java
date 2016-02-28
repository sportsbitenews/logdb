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

package org.araqne.logstorage;

import java.util.List;

public abstract class LogTraverseCallback {
	public enum BlockSkipReason {
		Reserved, Broken
	}

	private final Sink sink;
	private Throwable failure;

	abstract public void interrupt();

	abstract public boolean isInterrupted();

	public boolean isEof() {
		return sink.isEof() || isInterrupted();
	}

	public LogTraverseCallback(Sink sink) {
		this.sink = sink;
	}

	public boolean isOrdered() {
		return sink.isOrdered();
	}

	public void writeLogs(LogVectors logs) {
		// TODO: convert to list
		sink.write(logs);
	}

	public void writeLogs(List<Log> logs) {
		if (!isInterrupted())
			sink.write(filter(logs));
	}

	public boolean isFailed() {
		return failure != null;
	}

	public void setFailure(Throwable t) {
		failure = t;
	}

	public Throwable getFailure() {
		return failure;
	}

	abstract protected List<Log> filter(List<Log> logs);

	public interface VectorizedSink {
		void processLogs(LogVectors logs);
	}

	public static abstract class Sink {
		private final long offset;
		private final long limit;
		private final boolean ordered;
		private final boolean vectorized;

		/** guarded by this */
		private long curr;
		/** guarded by this */
		private boolean eof;

		public Sink(long offset, long limit) {
			this(offset, limit, true);
		}

		/**
		 * if ordered can be false, processLogs() and onBlockSkipped() should be
		 * thread-safe.
		 */
		public Sink(long offset, long limit, boolean order) {
			this.vectorized = this instanceof VectorizedSink;
			this.offset = offset;
			this.limit = limit;
			this.ordered = order;

			this.curr = 0;
			this.eof = false;
		}

		public synchronized boolean isEof() {
			return eof;
		}

		public boolean isOrdered() {
			return ordered;
		}

		public boolean write(LogVectors logs) {
			int processEnd = logs.size;
			long start = -1;
			long end = -1;
			synchronized (this) {
				if (eof)
					return false;

				start = curr;
				end = curr += logs.size;

				if (limit > 0 && end >= offset + limit) {
					processEnd = (int) (offset + limit - start);
					eof = true;
				}
			}

			if (offset > 0 && end <= offset)
				return true;

			int processBegin = 0;
			if (offset > 0 && start <= offset) {
				processBegin = (int) (offset - start);
			}

			if (processBegin == 0 && processEnd == logs.size) {
				if (vectorized) 
				((VectorizedSink) (this)).processLogs(logs);
				else
					processLogs(logs.toLogList());
				
			} else {
				int selectedSize = processEnd - processBegin;
				int[] selected = new int[selectedSize];
				int index = 0;
				for (int i = processBegin; i < processEnd; i++) {
					selected[index++] = i;
				}

				logs.selectedInUse = true;
				logs.selected = selected;
				logs.size = selectedSize;

				((VectorizedSink) (this)).processLogs(logs);
			}

			return !isEof();
		}

		/**
		 * @param logs
		 * @return true when it is not closed false when it is closed
		 */
		public boolean write(List<Log> logs) {
			if (logs.isEmpty())
				return !isEof();

			int processEnd = logs.size();

			int cnt = logs.size();
			long start = -1;
			long end = -1;

			synchronized (this) {
				if (eof)
					return false;

				start = curr;
				end = curr += cnt;

				if (limit > 0 && end >= offset + limit) {
					processEnd = (int) (offset + limit - start);
					eof = true;
				}
			}

			if (offset > 0 && end <= offset)
				return true;

			int processBegin = 0;
			if (offset > 0 && start <= offset) {
				processBegin = (int) (offset - start);
			}

			if (processBegin == 0 && processEnd == logs.size())
				processLogs(logs);
			else
				processLogs(logs.subList(processBegin, processEnd));

			return !isEof();
		}

		/**
		 * this method should be thread-safe if it is used in not-ordered case.
		 */
		protected abstract void processLogs(List<Log> logs);

		protected void onBlockSkipped(BlockSkipReason reason, long firstId, int logCount) {
		}
	}

	public void onBlockSkipped(BlockSkipReason reason, long firstId, int logCount) {
		sink.onBlockSkipped(reason, firstId, logCount);
	}

}
