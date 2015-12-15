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

import org.araqne.logdb.AccountService;
import org.araqne.logdb.BypassResultFactory;
import org.araqne.logdb.DefaultQuery;
import org.araqne.logdb.FieldOrdering;
import org.araqne.logdb.Procedure;
import org.araqne.logdb.Query;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.QueryTask;
import org.araqne.logdb.Session;
import org.araqne.logdb.SubQueryCommand;
import org.araqne.logdb.SubQueryTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Proc extends QueryCommand implements FieldOrdering, SubQueryCommand {
	private final Logger slog = LoggerFactory.getLogger(Proc.class);

	private Procedure procedure;

	private Query subQuery;
	private SubQueryTask subQueryTask;
	private String commandString;
	private AccountService accountService;
	private Session session;

	public Proc(Procedure procedure, String commandString, AccountService accountService, QueryContext procCtx,
			List<QueryCommand> procCommands) {
		this.procedure = procedure;
		this.commandString = commandString;
		this.accountService = accountService;

		this.subQuery = new DefaultQuery(procCtx, procedure.getQueryString(), procCommands, new BypassResultFactory(this));
		this.subQueryTask = new SubQueryTask(subQuery);
	}

	@Override
	public String getName() {
		return "proc";
	}

	@Override
	public Query getSubQuery() {
		return subQuery;
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
		session = accountService.newSession(procedure.getOwner());
	}

	@Override
	public void onClose(QueryStopReason reason) {
		try {
			subQuery.cancel(reason);
		} catch (Throwable t) {
			slog.error("araqne logdb: cannot stop proc subquery [" + subQuery.getQueryString() + "]", t);
		}

		if (session != null)
			accountService.logout(session);
	}

	@Override
	public QueryTask getMainTask() {
		return subQueryTask;
	}

	@Override
	public String toString() {
		return commandString;
	}
}
