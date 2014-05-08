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
package org.araqne.logdb.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.araqne.api.ScriptContext;
import org.araqne.logdb.AccountService;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryService;
import org.araqne.logdb.QueryResultSet;
import org.araqne.logdb.Permission;
import org.araqne.logdb.Query;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.RunMode;
import org.araqne.logdb.SavedResult;
import org.araqne.logdb.SavedResultManager;
import org.araqne.logdb.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Console {
	private final Logger logger = LoggerFactory.getLogger(Console.class);
	private ScriptContext context;
	private QueryService queryService;
	private AccountService accountService;
	private SavedResultManager savedResultManager;
	private Session session;

	public Console(ScriptContext context, AccountService accountService, QueryService queryService,
			SavedResultManager savedResultManager) {
		this.context = context;
		this.accountService = accountService;
		this.queryService = queryService;
		this.savedResultManager = savedResultManager;
	}

	private String getPrompt(Session session) {
		return session.getLoginName() + "@logdb> ";
	}

	public void run(String loginName) {
		try {
			context.print("password? ");
			String password = context.readPassword();
			session = accountService.login(loginName, password);

			context.println("Araqne LogDB Console");
			context.println("Type \"help\" for more information");

			while (true) {
				context.print(getPrompt(session));
				String line = context.readLine().trim();
				if (line.trim().equals("quit") || line.trim().equals("exit"))
					break;

				handle(line);
			}
		} catch (InterruptedException e) {
			context.println("interrupted");
		} catch (Throwable t) {
			if (t.getMessage() != null)
				context.println(t.getMessage());
			else
				context.println(t.toString());
		} finally {
			if (session != null) {
				accountService.logout(session);
				context.println("logout");
			}
		}
	}

	private void handle(String line) {
		if (line.trim().isEmpty())
			return;

		try {
			String[] args = line.split(" ");
			String command = args[0].trim();

			if (command.equals("help")) {
				help();
			} else if (command.equals("create_account")) {
				if (args.length < 2)
					context.println("Usage: create_account [login name]");
				else
					createAccount(args[1]);
			} else if (command.equals("remove_account")) {
				if (args.length < 2)
					context.println("Usage: remove_account [login name]");
				else
					removeAccount(args[1]);
			} else if (command.equals("passwd")) {
				if (args.length < 2)
					context.println("Usage: passwd [login name]");
				else
					changePassword(args[1]);
			} else if (command.equals("exec")) {
				if (args.length < 2)
					context.println("Usage: exec [file path]");
				else
					fileQuery(args[1]);
			} else if (command.equals("queries")) {
				queries(args.length > 1 ? args[1] : null);
			} else if (command.equals("query")) {
				query(line.substring("query".length()).trim());
			} else if (command.equals("create_query")) {
				createQuery(line.substring("create_query".length()).trim());
			} else if (command.equals("start_query")) {
				if (args.length < 2)
					context.println("Usage: start_query [query id]");
				else
					startQuery(Integer.valueOf(args[1]));
			} else if (command.equals("stop_query")) {
				if (args.length < 2)
					context.println("Usage: stop_query [query id]");
				else
					stopQuery(Integer.valueOf(args[1]));
			} else if (command.equals("remove_query")) {
				if (args.length < 2)
					context.println("Usage: remove_query [query id]");
				else
					removeQuery(Integer.valueOf(args[1]));
			} else if (command.equals("remove_all_queries")) {
				removeAllQueries();
			} else if (command.equals("fetch")) {
				if (args.length < 4)
					context.println("Usage: fetch [query id] [offset] [limit]");
				else {
					int id = Integer.valueOf(args[1]);
					long offset = Long.valueOf(args[2]);
					long limit = Long.valueOf(args[3]);
					fetch(id, offset, limit);
				}
			} else if (command.equals("grant_admin")) {
				if (args.length < 2)
					context.println("Usage: grant_admin [login name]");
				else
					grantAdmin(args[1]);
			} else if (command.equals("revoke_admin")) {
				if (args.length < 2)
					context.println("Usage: revoke_admin [login name]");
				else
					revokeAdmin(args[1]);
			} else if (command.equals("grant")) {
				if (args.length < 3)
					context.println("Usage: grant [login name] [table name]");
				else
					grantPrivilege(args[1], args[2]);
			} else if (command.equals("revoke")) {
				if (args.length < 3)
					context.println("Usage: revoke [login name] [table name]");
				else
					revokePrivilege(args[1], args[2]);
			} else if (command.equals("bg")) {
				if (args.length < 2)
					context.println("Usage: bg [query id]");
				else
					setRunMode(true, args[1]);
			} else if (command.equals("fg")) {
				if (args.length < 2)
					context.println("Usage: fg [query id]");
				else
					setRunMode(false, args[1]);
			} else if (command.equals("save")) {
				if (args.length < 3)
					context.println("Usage: save [query id] [title]");
				else
					save(args[1], args[2]);
			} else {
				context.println("invalid syntax");
			}
		} catch (Throwable t) {
			context.println(t.getMessage());
			logger.error("araqne logdb: console fail", t);
		}
	}

	private void help() {
		context.println("queries");
		context.println("\tprint all queries initiated by this session");

		context.println("query <query string>");
		context.println("\tcreate, start and fetch query result at once");

		context.println("create_query <query string>");
		context.println("\tcreate query with specified query string, and return allocated query id");

		context.println("start_query <query id>");
		context.println("\tstart query");

		context.println("stop_query <query_id>");
		context.println("\tstop running query");

		context.println("remove_query <query_id>");
		context.println("\tstop and remove query");

		context.println("fetch <query_id> <offset> <limit>");
		context.println("\tfetch result set of specified window. you can fetch partial result before query is ended");

		context.println("grant_admin <account>");
		context.println("\tgrant admin role to specified account");

		context.println("revoke_admin <account>");
		context.println("\trevoke admin role from specified account");

		context.println("grant <account> <table>");
		context.println("\tgrant read table permission to specified account");

		context.println("revoke <account> <table>");
		context.println("\trevoke read table permission from specified account");

		context.println("fg <query_id>");
		context.println("\trun query in foreground");

		context.println("bg <query_id>");
		context.println("\trun query as a background task");
	}

	private void createAccount(String loginName) throws InterruptedException {
		context.print("New password: ");
		String password = context.readPassword();
		accountService.createAccount(session, loginName, password);
		context.println("created " + loginName);
	}

	private void removeAccount(String loginName) {
		accountService.removeAccount(session, loginName);
		context.println("removed " + loginName);
	}

	private void changePassword(String loginName) throws InterruptedException {
		context.println("Changing password for user " + loginName);
		if (!session.isAdmin()) {
			context.print("(current) password: ");
			String current = context.readPassword();

			if (!accountService.verifyPassword(loginName, current)) {
				context.println("password mismatch");
				return;
			}
		}

		context.print("New password: ");
		String password = context.readPassword();

		context.print("Retype new password: ");
		String rePassword = context.readPassword();

		if (!password.equals(rePassword)) {
			context.println("Sorry, passwords do not match");
			return;
		}

		accountService.changePassword(session, loginName, rePassword);
		context.println("password changed");
	}

	private void fileQuery(String filePath) throws IOException {
		File f = new File(filePath);
		if (!f.exists()) {
			context.println("query file not found: " + f.getAbsolutePath());
			return;
		}

		if (!f.canRead()) {
			context.println("check query file permission: " + f.getAbsolutePath());
			return;
		}

		BufferedReader br = null;
		FileInputStream is = null;
		StringBuilder sb = new StringBuilder();
		try {
			is = new FileInputStream(f);
			br = new BufferedReader(new InputStreamReader(is, "utf-8"));

			while (true) {
				String line = br.readLine();
				if (line == null)
					break;

				if (!line.trim().startsWith("#"))
					sb.append(" " + line);
			}
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException e) {
				}
			}
		}
		String query = sb.toString();

		query(query);
	}

	private void queries(String queryFilter) {
		QueryPrintHelper.printQueries(context, queryService.getQueries(session), queryFilter);
	}

	private void query(String queryString) throws IOException {
		List<String> lines = new ArrayList<String>();
		try {
			queryString = queryString.trim();
			lines.add(queryString);
			if (queryString.endsWith("\\")) {
				while (true) {
					queryString = context.readLine().trim();
					lines.add(queryString);
					if (!queryString.endsWith("\\"))
						break;
				}
			}
		} catch (InterruptedException e) {
			return;
		}

		int i = 0;
		StringBuilder sb = new StringBuilder();
		for (String line : lines) {
			if (i++ != 0)
				sb.append(" ");

			if (line.endsWith("\\"))
				sb.append(line.substring(0, line.length() - 1));
			else
				sb.append(line);
		}

		queryString = sb.toString();

		long begin = System.currentTimeMillis();
		Query lq = queryService.createQuery(session, queryString);
		queryService.startQuery(session, lq.getId());

		do {
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
			}
		} while (!lq.isFinished());

		long count = 0;
		QueryResultSet rs = null;
		try {
			rs = lq.getResultSet();
			while (rs.hasNext()) {
				printMap(rs.next());
				count++;
			}
		} finally {
			if (rs != null)
				rs.close();
		}

		queryService.removeQuery(lq.getId());
		context.println(String.format("total %d rows, elapsed %.1fs", count, (System.currentTimeMillis() - begin) / (double) 1000));
	}

	private void createQuery(String queryString) {
		Query q = queryService.createQuery(session, queryString);
		context.println("created query " + q.getId());
	}

	private void startQuery(int id) {
		Query q = queryService.getQuery(session, id);
		if (q == null) {
			context.println("query not found");
			return;
		}

		queryService.startQuery(session, q.getId());
		context.println("started query " + id);
	}

	@SuppressWarnings("unchecked")
	private void printMap(Map<String, Object> m) {
		boolean start = true;
		context.print("{");
		List<String> keySet = new ArrayList<String>(m.keySet());
		Collections.sort(keySet);
		for (String key : keySet) {
			if (start)
				start = false;
			else
				context.print(", ");

			context.print(key + "=");
			Object value = m.get(key);
			if (value instanceof Map)
				printMap((Map<String, Object>) value);
			else if (value == null)
				context.print("null");
			else if (value.getClass().isArray()) {
				Class<?> c = value.getClass().getComponentType();
				if (c == byte.class)
					context.print(encodeBinary((byte[]) value));
				else
					context.print(Arrays.toString((Object[]) value));
			} else
				context.print(value.toString());
		}
		context.println("}");
	}

	private String encodeBinary(byte[] b) {
		StringBuilder sb = new StringBuilder(b.length * 2);
		for (int i = 0; i < b.length; i++) {
			sb.append(String.format("%02x", b[i]));
		}
		return sb.toString();
	}

	private void stopQuery(int id) {
		Query q = queryService.getQuery(session, id);
		if (q != null) {
			q.stop(QueryStopReason.UserRequest);
			context.println("stopped");
		} else {
			context.println("query not found: " + id);
		}
	}

	private void removeQuery(int id) {
		queryService.removeQuery(session, id);
		context.println("removed query " + id);
	}

	private void fetch(int id, long offset, long limit) throws IOException {
		Query q = queryService.getQuery(session, id);
		if (q == null) {
			context.println("query not found");
			return;
		}

		QueryResultSet result = q.getResultSet();
		result.skip(offset);
		for (long i = 0; result.hasNext() && i < limit; i++)
			printMap(result.next());
	}

	private void removeAllQueries() {
		for (Query q : queryService.getQueries(session)) {
			int id = q.getId();
			queryService.removeQuery(session, id);
			context.println("removed query " + id);
		}
		context.println("cleared all queries");
	}

	/**
	 * @since 1.0.0
	 */
	private void grantAdmin(String loginName) {
		accountService.grantAdmin(session, loginName);
		context.println("granted");
	}

	/**
	 * @since 1.0.0
	 */
	private void revokeAdmin(String loginName) {
		accountService.revokeAdmin(session, loginName);
		context.println("revoked");
	}

	private void grantPrivilege(String loginName, String tableName) {
		accountService.grantPrivilege(session, loginName, tableName, Permission.READ);
		context.println("granted");
	}

	private void revokePrivilege(String loginName, String tableName) {
		accountService.revokePrivilege(session, loginName, tableName, Permission.READ);
		context.println("revoked");
	}

	private void setRunMode(boolean background, String queryId) {
		int id = Integer.valueOf(queryId);

		Query q = queryService.getQuery(id);
		q.setRunMode(background ? RunMode.BACKGROUND : RunMode.FOREGROUND, new QueryContext(session));
		context.println(background ? "run as a background task" : "run in the foreground");
	}

	private void save(String queryId, String title) {
		int id = Integer.valueOf(queryId);

		Query q = queryService.getQuery(id);
		if (q == null) {
			context.println("query not found");
			return;
		}

		QueryResultSet rs = null;
		try {
			rs = q.getResultSet();
			long total = rs.getIndexPath().length() + rs.getDataPath().length();

			SavedResult sr = new SavedResult();
			sr.setStorageName(rs.getStorageName());
			sr.setOwner(session.getLoginName());
			sr.setQueryString(q.getQueryString());
			sr.setTitle(title);
			sr.setIndexPath(rs.getIndexPath().getAbsolutePath());
			sr.setDataPath(rs.getDataPath().getAbsolutePath());
			sr.setRowCount(rs.size());
			sr.setFileSize(total);

			savedResultManager.saveResult(sr);
			context.println("saved " + total + " bytes");
		} catch (IOException e) {
			logger.error("araqne logdb: cannot save result", e);
			context.println("save failed: " + e.getMessage());
		} finally {
			if (rs != null)
				rs.close();
		}
	}
}