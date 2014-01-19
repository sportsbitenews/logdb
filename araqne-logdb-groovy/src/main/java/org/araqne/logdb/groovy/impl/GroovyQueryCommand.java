/*
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
package org.araqne.logdb.groovy.impl;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.RowPipe;
import org.araqne.logdb.groovy.GroovyQueryScript;

public class GroovyQueryCommand extends QueryCommand {

	private GroovyQueryScript script;
	private ScriptPipe pipe = new ScriptPipe();

	public GroovyQueryCommand(GroovyQueryScript script) {
		this.script = script;
		this.script.setRowPipe(pipe);
	}

	@Override
	public String getName() {
		return "groovy";
	}

	@Override
	public void onPush(Row row) {
		script.onRow(row);
	}

	@Override
	public void onPush(RowBatch rowBatch) {
		script.onRowBatch(rowBatch);
	}

	@Override
	public void onClose(QueryStopReason reason) {
		script.onClose(reason);
	}

	private class ScriptPipe implements RowPipe {
		@Override
		public boolean isThreadSafe() {
			return true;
		}

		@Override
		public void onRow(Row row) {
			pushPipe(row);
		}

		@Override
		public void onRowBatch(RowBatch rowBatch) {
			pushPipe(rowBatch);
		}
	}
}
