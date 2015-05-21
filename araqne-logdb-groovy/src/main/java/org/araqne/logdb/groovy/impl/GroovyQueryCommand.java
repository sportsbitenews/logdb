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
import org.araqne.logdb.ThreadSafe;
import org.araqne.logdb.groovy.GroovyQueryScript;

public class GroovyQueryCommand extends QueryCommand implements ThreadSafe {

	private GroovyQueryScript script;
	private ScriptPipe pipe = new ScriptPipe();
	private final boolean isThreadSafe;

	public GroovyQueryCommand(GroovyQueryScript script) {
		this.script = script;
		this.script.setRowPipe(pipe);
		this.isThreadSafe = script instanceof ThreadSafe;
	}

	@Override
	public String getName() {
		return "groovy";
	}

	/**
	 * @since 0.1.3
	 */
	@Override
	public void onStart() {
		if (isThreadSafe) {
			script.onStart();
		} else {
			synchronized (script) {
				script.onStart();
			}
		}
	}

	@Override
	public void onPush(Row row) {
		if (isThreadSafe) {
			script.onRow(row);
		} else {
			synchronized (script) {
				script.onRow(row);
			}
		}
	}

	@Override
	public void onPush(RowBatch rowBatch) {
		if (isThreadSafe) {
			script.onRowBatch(rowBatch);
		} else {
			synchronized (script) {
				script.onRowBatch(rowBatch);
			}
		}
	}

	@Override
	public void onClose(QueryStopReason reason) {
		if (isThreadSafe) {
			script.onClose(reason);
		} else {
			synchronized (script) {
				script.onClose(reason);
			}
		}
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
