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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

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
import org.araqne.logdb.Permission;
import org.araqne.logdb.Procedure;
import org.araqne.logdb.ProcedureParameter;
import org.araqne.logdb.ProcedureRegistry;
import org.araqne.logdb.Query;
import org.araqne.logdb.QueryScriptFactory;
import org.araqne.logdb.QueryScriptRegistry;
import org.araqne.logdb.QueryService;
import org.araqne.logdb.SavedResult;
import org.araqne.logdb.SavedResultManager;
import org.araqne.logdb.SecurityGroup;
import org.araqne.logdb.Session;
import org.araqne.logdb.Strings;
import org.araqne.logstorage.LogTableRegistry;

public class LogDBScript implements Script {
	private QueryService qs;
	private QueryScriptRegistry scriptRegistry;
	private CsvLookupRegistry csvRegistry;
	private ScriptContext context;
	private LookupHandlerRegistry lookup;
	private AccountService accountService;
	private SavedResultManager savedResultManager;
	private ProcedureRegistry procedureRegistry;
	private LogTableRegistry tableRegistry;

	public LogDBScript(QueryService qs, QueryScriptRegistry scriptRegistry, LookupHandlerRegistry lookup,
			CsvLookupRegistry csvRegistry, AccountService accountService, SavedResultManager savedResultManager,
			ProcedureRegistry procedureRegistry, LogTableRegistry tableRegistry) {
		this.qs = qs;
		this.scriptRegistry = scriptRegistry;
		this.lookup = lookup;
		this.csvRegistry = csvRegistry;
		this.accountService = accountService;
		this.savedResultManager = savedResultManager;
		this.procedureRegistry = procedureRegistry;
		this.tableRegistry = tableRegistry;
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
			Procedure proc = inputProcedure(null);

			procedureRegistry.createProcedure(proc);
			context.println("created");
		} catch (InterruptedException e) {
			context.println("");
			context.println("interrupted");
		}
	}

	public void updateProcedure(String[] args) {
		try {
			String name = readLine("name", null);
			Procedure old = procedureRegistry.getProcedure(name);
			if (old == null) {
				context.println("procedure not found: " + name);
				return;
			}

			Procedure proc = inputProcedure(old);
			proc.setCreated(old.getCreated());
			procedureRegistry.updateProcedure(proc);
			context.println("updated");
		} catch (InterruptedException e) {
			context.println("");
			context.println("interrupted");
		}
	}

	@ScriptUsage(description = "grant procedure", arguments = {
			@ScriptArgument(name = "procedure name", type = "string", description = "procedure name"),
			@ScriptArgument(name = "type", type = "string", description = "'user' or 'group'"),
			@ScriptArgument(name = "login name", type = "string", description = "login name") })
	public void grantProcedure(String[] args) {
		String procName = args[0];
		String type = args[1];
		String target = args[2];

		Procedure p = procedureRegistry.getProcedure(procName);
		if (p == null) {
			context.println("procedure not found");
			return;
		}

		if (type.equals("user")) {
			if (!p.getGrants().contains(target)) {
				p.getGrants().add(target);
				procedureRegistry.updateProcedure(p);
			}
		} else if (type.equals("group")) {
			Map<String, SecurityGroup> groupMap = getSecurityGroupMap();
			SecurityGroup old = groupMap.get(target);
			if (old != null) {
				p.getGrantGroups().add(old.getGuid());
				procedureRegistry.updateProcedure(p);
			}
		} else {
			context.println("invalid type. use 'user' or 'group'");
			return;
		}

		context.println("granted");
	}

	@ScriptUsage(description = "revoke procedure", arguments = {
			@ScriptArgument(name = "procedure name", type = "string", description = "procedure name"),
			@ScriptArgument(name = "type", type = "string", description = "user or group"),
			@ScriptArgument(name = "login name", type = "string", description = "login name") })
	public void revokeProcedure(String[] args) {
		String procName = args[0];
		String type = args[1];
		String target = args[2];

		Procedure p = procedureRegistry.getProcedure(procName);
		if (p == null) {
			context.println("procedure not found");
			return;
		}

		if (type.equals("user")) {
			p.getGrants().remove(target);
			procedureRegistry.updateProcedure(p);
		} else if (type.equals("group")) {
			Map<String, SecurityGroup> groupMap = getSecurityGroupMap();
			SecurityGroup old = groupMap.get(target);
			if (old != null) {
				p.getGrantGroups().remove(old.getGuid());
				procedureRegistry.updateProcedure(p);
			}
		} else {
			context.println("invalid type. use 'user' or 'group'");
			return;
		}

		context.println("revoked");
	}

	private Procedure inputProcedure(Procedure old) throws InterruptedException {
		Procedure proc = new Procedure();

		if (old == null)
			proc.setName(readLine("name", null));
		else
			proc.setName(old.getName());

		proc.setOwner(readLine("owner", old != null ? old.getOwner() : null));
		proc.setGrants(new HashSet<String>(Strings.tokenize(
				readLine("grants", old != null ? Strings.join(old.getGrants(), ", ") : null), ",")));
		proc.setQueryString(readLine("query", old != null ? old.getQueryString() : null));

		List<ProcedureParameter> parameters = new ArrayList<ProcedureParameter>();
		context.println("Type parameter definitions in \"type name\" format. e.g. \"string opt\", press enter to end.");
		int idx = 0;
		while (true) {
			ProcedureParameter oldParam = null;
			if (old != null && idx < old.getParameters().size()) {
				oldParam = old.getParameters().get(idx);
			}

			context.print("parameter? ");
			String line = context.readLine(oldParam != null ? oldParam.toString() : null);
			if (line.isEmpty())
				break;

			int p = line.indexOf(" ");
			String type = line.substring(0, p).trim();
			String key = line.substring(p + 1).trim();

			parameters.add(new ProcedureParameter(key, type));
			idx++;
		}

		proc.setParameters(parameters);

		return proc;
	}

	private String readLine(String prompt, String old) throws InterruptedException {
		context.print(prompt + "? ");
		return context.readLine(old);
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

	public void securityGroups(String[] args) {
		context.println("Security Groups");
		context.println("-----------------");

		for (SecurityGroup group : accountService.getSecurityGroups()) {
			context.println(group);
		}
	}

	@ScriptUsage(description = "create security group", arguments = {
			@ScriptArgument(name = "group name", type = "string", description = "name of security group"),
			@ScriptArgument(name = "description", type = "string", description = "description", optional = true) })
	public void createSecurityGroup(String[] args) {
		SecurityGroup group = new SecurityGroup();
		group.setName(args[0]);
		if (args.length > 1)
			group.setDescription(args[1]);

		accountService.createSecurityGroup(null, group);
		context.println("created");
	}

	@ScriptUsage(description = "remove security group", arguments = { @ScriptArgument(name = "group name", type = "string", description = "name of security group") })
	public void removeSecurityGroup(String[] args) {
		SecurityGroup found = findSecurityGroupByName(args[0]);
		if (found == null) {
			context.println("security group not found");
			return;
		}

		accountService.removeSecurityGroup(null, found.getGuid());
		context.println("removed");
	}

	@ScriptUsage(description = "join security group", arguments = {
			@ScriptArgument(name = "group name", type = "string", description = "name of security group"),
			@ScriptArgument(name = "login name", type = "string", description = "account login name") })
	public void joinSecurityGroup(String[] args) {
		SecurityGroup group = findSecurityGroupByName(args[0]);
		if (group == null) {
			context.println("security group not found");
			return;
		}

		for (int i = 1; i < args.length; i++) {
			String loginName = args[i];
			Account account = accountService.getAccount(loginName);
			if (account == null) {
				context.println("account [" + loginName + "] not found. skipping.");
				continue;
			}

			group.getAccounts().add(account.getLoginName());
		}

		accountService.updateSecurityGroup(null, group);
		context.println("updated");
	}

	@ScriptUsage(description = "leave security group", arguments = {
			@ScriptArgument(name = "group name", type = "string", description = "name of security group"),
			@ScriptArgument(name = "login name", type = "string", description = "account login name") })
	public void leaveSecurityGroup(String[] args) {
		SecurityGroup group = findSecurityGroupByName(args[0]);
		if (group == null) {
			context.println("security group not found");
			return;
		}

		for (int i = 1; i < args.length; i++)
			group.getAccounts().remove(args[i]);

		accountService.updateSecurityGroup(null, group);
		context.println("updated");
	}

	@ScriptUsage(description = "grant table access", arguments = {
			@ScriptArgument(name = "table name", type = "string", description = "table name"),
			@ScriptArgument(name = "type", type = "string", description = "user or group"),
			@ScriptArgument(name = "login name or group name", type = "string", description = "name of user or security group") })
	public void grantTable(String[] args) {
		String tableName = args[0];
		String type = args[1];

		if (!tableRegistry.exists(tableName)) {
			context.println("table not found: " + tableName);
			return;
		}

		if (!type.equals("user") && !type.equals("group")) {
			context.println("invalid type, use 'user' or 'group'");
			return;
		}

		if (type.equals("user")) {
			for (int i = 2; i < args.length; i++) {
				String loginName = args[i];
				if (accountService.getAccount(loginName) == null) {
					context.println("account [" + loginName + "] not found, skipping");
					continue;
				}

				accountService.grantPrivilege(null, loginName, tableName, Permission.READ);

			}
		} else if (type.equals("group")) {
			Map<String, SecurityGroup> groupMap = getSecurityGroupMap();
			for (int i = 2; i < args.length; i++) {
				String groupName = args[i];
				SecurityGroup group = groupMap.get(groupName);
				if (group == null) {
					context.println("security group [" + groupName + "] not found, skipping");
					continue;
				}

				group.getReadableTables().add(tableName);
				accountService.updateSecurityGroup(null, group);
			}
		}

		context.println("granted");
	}

	@ScriptUsage(description = "revoke table access from group", arguments = {
			@ScriptArgument(name = "table name", type = "string", description = "table name"),
			@ScriptArgument(name = "type", type = "string", description = "user or group"),
			@ScriptArgument(name = "login name or group name", type = "string", description = "name of user or security group") })
	public void revokeTable(String[] args) {
		String tableName = args[0];
		String type = args[1];

		if (!tableRegistry.exists(tableName)) {
			context.println("table not found: " + tableName);
			return;
		}

		if (!type.equals("user") && !type.equals("group")) {
			context.println("invalid type, use 'user' or 'group'");
			return;
		}

		if (type.equals("user")) {
			for (int i = 2; i < args.length; i++) {
				String loginName = args[i];
				if (accountService.getAccount(loginName) == null) {
					context.println("account [" + loginName + "] not found, skipping");
					continue;
				}

				accountService.revokePrivilege(null, loginName, tableName, Permission.READ);

			}
		} else if (type.equals("group")) {
			Map<String, SecurityGroup> groupMap = getSecurityGroupMap();
			for (int i = 2; i < args.length; i++) {
				String groupName = args[i];
				SecurityGroup group = groupMap.get(groupName);
				if (group == null) {
					context.println("security group [" + groupName + "] not found, skipping");
					continue;
				}

				group.getReadableTables().remove(tableName);
				accountService.updateSecurityGroup(null, group);
			}
		}

		context.println("revoked");
	}

	// name to security group map
	private Map<String, SecurityGroup> getSecurityGroupMap() {
		Map<String, SecurityGroup> m = new HashMap<String, SecurityGroup>();
		for (SecurityGroup group : accountService.getSecurityGroups()) {
			m.put(group.getName(), group);
		}
		return m;
	}

	private SecurityGroup findSecurityGroupByName(String name) {
		SecurityGroup found = null;
		for (SecurityGroup group : accountService.getSecurityGroups()) {
			if (group.getName().equals(name)) {
				found = group;
				break;
			}
		}
		return found;
	}

}
