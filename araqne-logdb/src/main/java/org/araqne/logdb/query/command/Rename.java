/*
 * Copyright 2011 Future Systems
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
package org.araqne.logdb.query.command;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.ThreadSafe;

public class Rename extends QueryCommand implements ThreadSafe {
	public static class Pair {
		private final String from;
		private final String to;
		
		public Pair(String from, String to) {
			this.from = from;
			this.to = to;
		}
		
		public String getFrom() {
			return from;
		}
		
		public String getTo() {
			return to;
		}
	}
	
	private final Iterable<Pair> pairs;
	
	public Rename(Iterable<Pair> pairs) {
		this.pairs = pairs;
	}

	@Override
	public String getName() {
		return "rename";
	}

	public Iterable<Pair> getPairs() {
		return pairs;
	}
	
	@Override
	public void onPush(Row row) {
		for (Pair pair : pairs) {
			if (row.containsKey(pair.from))
				row.put(pair.to, row.remove(pair.from));
		}
		pushPipe(row);
	}

	@Override
	public void onPush(RowBatch rowBatch) {
		if (rowBatch.selectedInUse) {
			for (int i = 0; i < rowBatch.size; i++) {
				int p = rowBatch.selected[i];
				Row row = rowBatch.rows[p];
				for (Pair pair : pairs) {
					if (row.containsKey(pair.from))
						row.put(pair.to, row.remove(pair.from));
				}
			}
		} else {
		    for (int i = 0; i < rowBatch.size; i++) {
		        Row row = rowBatch.rows[i];

				for (Pair pair : pairs) {
					if (row.containsKey(pair.from))
						row.put(pair.to, row.remove(pair.from));
				}
			}
		}

		pushPipe(rowBatch);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("rename ");
		boolean first = true;
		
		for (Pair pair : pairs) {
			if (first) {
				first = false;
			} else {
				sb.append(", ");
			}
			
			sb.append(pair.from);
			sb.append(" as ");
			sb.append(pair.to);
		}
		
		return sb.toString();
	}
}
