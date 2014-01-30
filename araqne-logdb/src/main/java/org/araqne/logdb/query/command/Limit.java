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
package org.araqne.logdb.query.command;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.Row;

/**
 * @since 1.7.2
 * @author xeraph
 * 
 */
public class Limit extends QueryCommand {

	private final long offset;
	private final long limit;

	private long skip;
	private long count;

	public Limit(long offset, long limit) {
		this.offset = offset;
		this.limit = limit;
	}

	@Override
	public String getName() {
		return "limit";
	}

	public long getOffset() {
		return offset;
	}

	public long getLimit() {
		return limit;
	}

	@Override
	public void onPush(Row m) {
		if (skip < offset) {
			skip++;
			return;
		}

		if (count < limit) {
			pushPipe(m);
			count++;
		} else {
			getQuery().stop(QueryStopReason.PartialFetch);
		}
	}

	@Override
	public boolean isReducer() {
		return true;
	}

	@Override
	public String toString() {
		if (offset == 0)
			return "limit " + limit;
		return "limit " + offset + " " + limit;
	}

}
