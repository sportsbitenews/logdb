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
package org.araqne.logdb.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.araqne.api.PathAutoCompleter;
import org.araqne.api.Script;
import org.araqne.api.ScriptArgument;
import org.araqne.api.ScriptContext;
import org.araqne.api.ScriptUsage;
import org.araqne.logdb.Account;
import org.araqne.logdb.AccountService;
import org.araqne.logdb.CsvLookupRegistry;
import org.araqne.logdb.ExternalAuthService;
import org.araqne.logdb.LookupHandlerRegistry;
import org.araqne.logdb.Procedure;
import org.araqne.logdb.ProcedureRegistry;
import org.araqne.logdb.ProcedureParameter;
import org.araqne.logdb.Query;
import org.araqne.logdb.QueryScriptFactory;
import org.araqne.logdb.QueryScriptRegistry;
import org.araqne.logdb.QueryService;
import org.araqne.logdb.SavedResult;
import org.araqne.logdb.SavedResultManager;
import org.araqne.logdb.Session;

public class LogDBScript implements Script {
	private QueryService qs;
	private QueryScriptRegistry scriptRegistry;
	private CsvLookupRegistry csvRegistry;
	private ScriptContext context;
	private LookupHandlerRegistry lookup;
	private AccountService accountService;
	private SavedResultManager savedResultManager;
	private ProcedureRegistry procedureRegistry;

	public LogDBScript(QueryService qs, QueryScriptRegistry scriptRegistry, LookupHandlerRegistry lookup,
			CsvLookupRegistry csvRegistry, AccountService accountService, SavedResultManager savedResultManager,
			ProcedureRegistry procedureRegistry) {
		this.qs = qs;
		this.scriptRegistry = scriptRegistry;
		this.lookup = lookup;
		this.csvRegistry = csvRegistry;
		this.accountService = accountService;
		this.savedResultManager = savedResultManager;
		this.procedureRegistry = procedureRegistry;
	}

	@Override
	public void setScriptContext(ScriptContext context) {
		this.context = context;
	}

	public void authServices(String[] args) {
		context.println("External Auth Services");
		context.println("------------------------");

		ExternalAuthService using = accountService.getUsingAuthService();
		for (ExternalAuthService s : accountService.getAuthServices()) {
			context.print(using == s ? "[*] " : "[ ] ");
			context.println(s.getName() + " - " + s);
		}
	}

	public void useAuthService(String[] args) {
		accountService.useAuthService(args.length > 0 ? args[0] : null);
		context.println(args.length > 0 ? "set" : "unset");
	}

	public void sessions(String[] args) {
		context.println("Current Sessions");
		context.println("------------------");
		for (Session session : accountService.getSessions()) {
			context.println(session);
		}
	}

	public void accounts(String[] args) {
		context.println("Accounts");
		context.println("----------");
		for (String loginName : accountService.getAccountNames()) {
			Account account = accountService.getAccount(loginName);
			String admin = "";
			if (account.isAdmin())
				admin = " (admin)";
			context.println(loginName + admin);
		}
	}

	@ScriptUsage(description = "open console", arguments = { @ScriptArgument(name = "login name", type = "string", description = "db account name") })
	public void console(String[] args) {
		new Console(context, accountService, qs, savedResultManager).run(args[0]);
	}

	public void csvLookups(String[] args) {
		context.println("CSV Mapping Files");
		context.println("-------------------");
		for (File f : csvRegistry.getCsvFiles()) {
			context.println(f.getAbsolutePath());
		}
	}

	@ScriptUsage(description = "create new log query script workspace", arguments = { @ScriptArgument(name = "workspace name", type = "string", description = "log query script workspace name") })
	public void createScriptWorkspace(String[] args) {
		scriptRegistry.createWorkspace(args[0]);
		context.println("created");
	}

	@ScriptUsage(description = "remove log query script workspace", arguments = { @ScriptArgument(name = "workspace name", type = "string", description = "log query script workspace name") })
	public void dropScriptWorkspace(String[] args) {
		scriptRegistry.dropWorkspace(args[0]);
		context.println("dropped");
	}

	@ScriptUsage(description = "load csv lookup mapping file", arguments = { @ScriptArgument(name = "path", type = "string", description = "csv (comma separated value) file path. first line should be column headers.", autocompletion = PathAutoCompleter.class) })
	public void loadCsvLookup(String[] args) throws IOException {
		try {
			File f = new File(args[0]);
			csvRegistry.loadCsvFile(f);
			context.println("loaded " + f.getAbsolutePath());
		} catch (IllegalStateException e) {
			context.println(e);
		}
	}

	@ScriptUsage(description = "reload csv lookup mapping file", arguments = { @ScriptArgument(name = "path", type = "string", description = "csv (comma separated value) file path. first line should be column headers.", autocompletion = PathAutoCompleter.class) })
	public void reloadCsvLookup(String[] args) throws IOException {
		try {
			File f = new File(args[0]);
			csvRegistry.unloadCsvFile(f);
			csvRegistry.loadCsvFile(f);
			context.println("reloaded");
		} catch (IllegalStateException e) {
			context.println(e);
		}
	}

	@ScriptUsage(description = "unload csv lookup mapping file", arguments = { @ScriptArgument(name = "path", type = "string", description = "registered csv file path", autocompletion = PathAutoCompleter.class) })
	public void unloadCsvLookup(String[] args) {
		File f = new File(args[0]);
		csvRegistry.unloadCsvFile(f);
		context.println("unloaded" + f.getAbsolutePath());
	}

	public void scripts(String[] args) {
		context.println("Log Scripts");
		context.println("--------------");

		for (String workspace : scriptRegistry.getWorkspaceNames()) {
			context.println("Workspace: " + workspace);
			for (String name : scriptRegistry.getScriptFactoryNames(workspace)) {
				QueryScriptFactory factory = scriptRegistry.getScriptFactory(workspace, name);
				context.println("  " + name + " - " + factory);
			}
		}
	}

	public void lookupHandlers(String[] args) {
		context.println("Lookup Handlers");
		context.println("---------------------");
		for (String name : lookup.getLookupHandlerNames())
			context.println(name);
	}

	/**
	 * @since 0.14.0
	 */
	public void queries(String[] args) {
		String queryFilter = null;
		if (args.length > 0)
			queryFilter = args[0];
		QueryPrintHelper.printQueries(context, qs.getQueries(), queryFilter);
	}

	/**
	 * @since 0.16.2
	 */
	@ScriptUsage(description = "print specific query status", arguments = { @ScriptArgument(name = "query id", type = "int", description = "query id") })
	public void queryStatus(String[] args) {
		Integer id = Integer.valueOf(args[0]);
		Query q = qs.getQuery(id);
		if (q == null) {
			context.println("query " + id + " not found");
			return;
		}

		context.println("Log Query Status");
		context.println("------------------");
		context.println(QueryPrintHelper.getQueryStatus(q));
	}

	/**
	 * @since 0.14.0
	 */
	@ScriptUsage(description = "remove query", arguments = { @ScriptArgument(name = "query id", type = "int", description = "query id") })
	public void removeQuery(String[] args) {
		for (String arg : args) {
			Integer id = Integer.valueOf(arg);
			if (qs.getQuery(id) != null) {
				qs.removeQuery(id);
				context.println("removed query " + arg);
			} else {
				context.println("query " + id + " not found");
			}
		}
	}

	/**
	 * @since 0.16.2
	 */
	public void removeAllQueries(String[] args) {
		for (Query q : qs.getQueries()) {
			qs.removeQuery(q.getId());
			context.println("removed query " + q.getId());
		}
	}

	/**
	 * @since 2.0.3
	 */
	@ScriptUsage(description = "delete saved query result", arguments = { @ScriptArgument(name = "guid", type = "string", description = "the guid of saved query result") })
	public void deleteSavedResult(String[] args) throws IOException {
		String guid = args[0];
		SavedResult sr = savedResultManager.getResult(guid);
		if (sr == null) {
			context.println("query result not found");
			return;
		}

		savedResultManager.deleteResult(guid);
		context.println("deleted '" + sr.getTitle() + "', " + sr.getFileSize() + " bytes, " + sr.getRowCount() + " rows");
	}

	/**
	 * @since 2.0.3
	 */
	@ScriptUsage(description = "list all saved query results", arguments = { @ScriptArgument(name = "owner", type = "string", description = "login name", optional = true) })
	public void savedResults(String[] args) {
		String owner = args.length > 0 ? args[0] : null;
		List<SavedResult> savedResults = savedResultManager.getResultList(owner);

		context.println("Saved Query Results");
		context.println("---------------------");

		for (SavedResult sr : savedResults) {
			context.println(sr);
		}

		context.println("total " + savedResults.size() + " results");
	}

	public void instanceGuid(String[] args) {
		context.println(accountService.getInstanceGuid());
	}

	public void setInstanceGuid(String[] args) {
		accountService.setInstanceGuid(args[0]);
	}

	public void procedures(String[] args) {
		context.println("Procedures");
		context.println("------------");

		for (Procedure p : procedureRegistry.getProcedures()) {
			int i = 0;
			String signature = p.getName() + "(";
			for (ProcedureParameter param : p.getParameters()) {
				if (i++ != 0)
					signature += ", ";
				signature += param;
			}
			signature += ")";

			context.println(signature + ", owner=" + p.getOwner());
		}
	}

	@ScriptUsage(description = "print procedure details", arguments = { @ScriptArgument(name = "name", type = "string", description = "procedure name") })
	public void procedure(String[] args) {
		Procedure p = procedureRegistry.getProcedure(args[0]);
		if (p == null) {
			context.println("procedure not found");
			return;
		}

		context.println(p);
	}

	public void createProcedure(String[] args) {
		try {
			Procedure proc = new Procedure();

			proc.setName(readLine("name"));
			proc.setOwner(readLine("owner"));
			proc.setQueryString(readLine("query"));

			List<ProcedureParameter> parameters = new ArrayList<ProcedureParameter>();
			context.println("Type parameter definitions in \"type name\" format. e.g. \"string opt\", press enter to end.");
			while (true) {
				context.print("parameter? ");
				String line = context.readLine();
				if (line.isEmpty())
					break;

				int p = line.indexOf(" ");
				String type = line.substring(0, p).trim();
				String key = line.substring(p + 1).trim();

				parameters.add(new ProcedureParameter(key, type));
			}

			proc.setParameters(parameters);

			procedureRegistry.createProcedure(proc);
			context.println("created");
		} catch (InterruptedException e) {
			context.println("");
			context.println("interrupted");
		}
	}

	private String readLine(String prompt) throws InterruptedException {
		context.print(prompt + "? ");
		return context.readLine();
	}

	@ScriptUsage(description = "remove procedure", arguments = { @ScriptArgument(name = "procedure name", type = "string", description = "procedure name") })
	public void removeProcedure(String[] args) {
		String name = args[0];
		Procedure p = procedureRegistry.getProcedure(name);
		if (p == null) {
			context.println("procedure not found: " + name);
			return;
		}

		procedureRegistry.removeProcedure(name);
		context.println("removed");
	}
	
	@ScriptUsage(description = "set hash join threshold", arguments = { @ScriptArgument(name = "threshold", type = "int", description = "hash join threshold") })
	public void setHashJoinThreshold(String[] args) {
		System.setProperty("araqne.hashjointhreshold", args[0]);
		context.println("set hash join threshold");
	}
	
	@ScriptUsage(description = "get hash join threshold")
	public void getHashJoinThreshold(String[] args) {
		context.println("hash join threshold :" + System.getProperty("araqne.hashjointhreshold", "100000"));
	}
}
