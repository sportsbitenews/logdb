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
package org.araqne.logdb.query.command;

import java.util.List;

import org.araqne.logdb.DefaultQuery;
import org.araqne.logdb.Procedure;
import org.araqne.logdb.Query;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.QueryTask;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.RowPipe;
import org.araqne.logdb.StreamResultFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Proc extends QueryCommand {
	private final Logger slog = LoggerFactory.getLogger(Proc.class);

	private ProcPipe procPipe = new ProcPipe();
	private ProcTask procTask = new ProcTask();
	private Query subQuery;
	private String commandString;

	public Proc(QueryContext procCtx, Procedure procedure, QueryParserService parserService, String commandString) {
		this.commandString = commandString;
		
		List<QueryCommand> procCommands = parserService.parseCommands(procCtx, procedure.getQueryString());
		this.subQuery = new DefaultQuery(procCtx, procedure.getQueryString(), procCommands, new StreamResultFactory(procPipe));

		for (QueryCommand cmd : subQuery.getCommands()) {
			if (cmd.getMainTask() != null) {
				procTask.addDependency(cmd.getMainTask());
				procTask.addSubTask(cmd.getMainTask());
			}
		}
	}

	@Override
	public String getName() {
		return "proc";
	}

	@Override
	public void onStart() {
		subQuery.preRun();
	}

	@Override
	public QueryTask getMainTask() {
		return procTask;
	}

	private class ProcPipe implements RowPipe {

		@Override
		public boolean isThreadSafe() {
			return false;
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

	@Override
	public String toString() {
		return commandString;
	}

	private class ProcTask extends QueryTask {
		@Override
		public void run() {
			slog.debug("araqne logdb: proc subquery end, main query [{}] sub query [{}]", query.getId(), subQuery.getId());
			subQuery.postRun();
		}
	}
}
