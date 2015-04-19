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
import java.util.Map;

import org.araqne.logdb.AccountService;
import org.araqne.logdb.DefaultQuery;
import org.araqne.logdb.FieldOrdering;
import org.araqne.logdb.Procedure;
import org.araqne.logdb.Query;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.QueryTask;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.RowPipe;
import org.araqne.logdb.Session;
import org.araqne.logdb.StreamResultFactory;
import org.araqne.logdb.impl.QueryHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Proc extends QueryCommand implements FieldOrdering {
	private final Logger slog = LoggerFactory.getLogger(Proc.class);

	private ProcPipe procPipe = new ProcPipe();
	private ProcTask procTask = new ProcTask();
	private Query subQuery;
	private String commandString;

	private AccountService accountService;
	private Session session;

	// depend and wait outer commands
	private Trigger triggerTask = new Trigger();

	public Proc(Procedure procedure, Map<String, Object> procParams, String commandString, QueryParserService parserService,
			AccountService accountService) {
		this.accountService = accountService;
		this.commandString = commandString;

		procTask.addSubTask(triggerTask);

		session = accountService.newSession(procedure.getOwner());
		QueryContext procCtx = new QueryContext(session);
		for (String key : procParams.keySet()) {
			Object value = procParams.get(key);
			Map<String, Object> constants = procCtx.getConstants();
			constants.put(key, value);
		}

		List<QueryCommand> procCommands = parserService.parseCommands(procCtx, procedure.getQueryString());
		this.subQuery = new DefaultQuery(procCtx, procedure.getQueryString(), procCommands, new StreamResultFactory(procPipe));
		QueryHelper.setJoinAndUnionDependencies(subQuery.getCommands());

		for (QueryCommand cmd : subQuery.getCommands()) {
			if (cmd.getMainTask() != null) {
				procTask.addDependency(cmd.getMainTask());
				cmd.getMainTask().addDependency(triggerTask);
				procTask.addSubTask(cmd.getMainTask());
			}
		}
	}

	@Override
	public String getName() {
		return "proc";
	}

	@Override
	public List<String> getFieldOrder() {
		return subQuery.getFieldOrder();
	}

	@Override
	public boolean isDriver() {
		return true;
	}

	@Override
	public void onStart() {
		subQuery.preRun();
	}

	@Override
	public void onClose(QueryStopReason reason) {
		if (session != null)
			accountService.logout(session);
	}

	@Override
	public QueryTask getMainTask() {
		return procTask;
	}

	@Override
	public QueryTask getDependency() {
		return triggerTask;
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

	private class Trigger extends QueryTask {
		@Override
		public void run() {
			slog.debug("araqne logdb: proc subquery started (dependency resolved), main query [{}] sub query [{}]",
					query.getId(), subQuery.getId());
		}
	}

	private class ProcTask extends QueryTask {
		@Override
		public void run() {
			slog.debug("araqne logdb: proc subquery end, main query [{}] sub query [{}]", query.getId(), subQuery.getId());
			subQuery.postRun();
		}
	}
}
