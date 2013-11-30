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

import org.araqne.logdb.DriverQueryCommand;
import org.araqne.logdb.MetadataCallback;
import org.araqne.logdb.MetadataService;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.Row;

public class Logdb extends DriverQueryCommand {
	private QueryContext context;
	private String objectType;
	private String args;
	private MetadataService metadataService;
	private MetadataCallbackWriter metadataWriter;
	private boolean completed;

	public Logdb(QueryContext context, String objectType, String args, MetadataService metadataService) {
		this.context = context;
		this.objectType = objectType;
		this.args = args;
		this.metadataService = metadataService;
		this.metadataWriter = new MetadataCallbackWriter();
	}

	@Override
	public void run() {
		metadataService.query(context, objectType, args, metadataWriter);
		completed = true;
	}

	@Override
	public void onClose(QueryStopReason reason) {
		if (!completed)
			metadataWriter.cancelled = true;
	}

	private class MetadataCallbackWriter implements MetadataCallback {
		private boolean cancelled;

		@Override
		public boolean isCancelled() {
			return cancelled;
		}

		@Override
		public void onPush(Row log) {
			pushPipe(log);
		}
	}

	@Override
	public String toString() {
		String arguments = args;
		if (!arguments.isEmpty())
			arguments = " " + arguments;
		return "logdb " + objectType + arguments;
	}
}
