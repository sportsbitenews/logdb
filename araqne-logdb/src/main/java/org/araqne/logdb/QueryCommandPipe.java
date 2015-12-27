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

public class QueryCommandPipe implements RowPipe {
	private QueryCommand dst;
	private final boolean threadSafe;

	public QueryCommandPipe(QueryCommand dst) {
		this.dst = dst;
		this.threadSafe = dst != null && dst instanceof ThreadSafe;
	}

	@Override
	public boolean isThreadSafe() {
		return threadSafe;
	}

	@Override
	public void onRow(Row row) {
		dst.onPush(row);
	}

	@Override
	public void onRowBatch(RowBatch rowBatch) {
		dst.onPush(rowBatch);
	}

	@Override
	public void onVectorizedRowBatch(VectorizedRowBatch vrowBatch) {
		dst.onPush(vrowBatch);
	}
}
