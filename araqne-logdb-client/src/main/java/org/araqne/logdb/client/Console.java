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
package org.araqne.logdb.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.client.http.CometClient;

public class Console {
	private CometClient client;
	private String host;
	private String loginName;
	private String password;

	public static void main(String[] args) throws IOException {
		new Console().run();
	}

	public void run() throws IOException {
		w("Araqne LogDB Console 0.2 (2013-03-08)");
		w("Type \"help\" for more information");

		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

		try {
			while (true) {
				System.out.print(getPrompt());
				String line = br.readLine();
				String[] tokens = line.split(" ");
				if (tokens.length == 0)
					continue;

				String cmd = tokens[0];
				if (cmd.equals("quit") || cmd.equals("exit"))
					break;
				else if (cmd.equals("help"))
					help();
				else if (cmd.equals("connect"))
					connect(tokens);
				else if (cmd.equals("disconnect"))
					disconnect();
				else if (cmd.equals("query"))
					query(tokens);
				else if (cmd.equals("create_query"))
					createQuery(tokens);
				else if (cmd.equals("start_query"))
					startQuery(tokens);
				else if (cmd.equals("stop_query"))
					stopQuery(tokens);
				else if (cmd.equals("remove_query"))
					removeQuery(tokens);
				else if (cmd.equals("fetch"))
					fetch(tokens);
				else if (cmd.equals("queries"))
					queries();
				else if (cmd.equals("create_table"))
					createTable(tokens);
				else if (cmd.equals("drop_table"))
					dropTable(tokens);
				else
					w("syntax error");

			}
		} finally {
			if (client != null) {
				w("closing logdb connection...");
				client.close();
				w("bye!");
			}
		}

	}

	private void connect(String[] tokens) {
		if (tokens.length < 4) {
			w("Usage: connect <host> <loginname> <password>");
			return;
		}

		if (client != null) {
			w("already connected");
			return;
		}

		host = tokens[1];

		try {
			InetAddress.getByName(host);
		} catch (UnknownHostException e) {
			w("invalid hostname " + host + ", connect failed");
			return;
		}

		loginName = tokens[2];
		password = tokens[3];

		try {
			client = new CometClient();
			client.connect(host, loginName, password);
			w("connected to " + host + " as " + loginName);
		} catch (Throwable t) {
			w(t.getMessage());
			if (client != null) {
				client.close();
				client = null;
			}
		}
	}

	private void disconnect() {
		if (client == null) {
			w("not connected yet");
			return;
		}

		w("closing connection...");
		client.close();
		w("disconnected");
		client = null;
	}

	private void queries() {
		if (client == null) {
			w("connect first please");
			return;
		}

		List<LogQuery> queries = client.getQueries();
		if (queries.size() == 0) {
			w("no result");
			return;
		}

		for (LogQuery query : queries) {
			w(query.toString());
		}
	}

	private void query(String[] tokens) {
		if (client == null) {
			w("connect first please");
			return;
		}

		String queryString = join(tokens);
		w("querying [" + queryString + "] ...");

		long count = 0;
		LogCursor cursor = null;
		try {
			cursor = client.query(queryString);
			while (cursor.hasNext()) {
				Object o = cursor.next();
				w(o.toString());
				count++;
			}

			w("total " + count + " row(s)");
		} catch (Throwable t) {
			w("query failed: " + t.getMessage());
		} finally {
			if (cursor != null) {
				try {
					cursor.close();
				} catch (IOException e) {
				}
			}
		}
	}

	private void createQuery(String[] tokens) {
		if (client == null) {
			w("connect first please");
			return;
		}

		if (tokens.length < 2) {
			w("Usage: create_query <query_string>");
			return;
		}

		try {
			String queryString = join(tokens);
			int id = client.createQuery(queryString);
			w("created query " + id);
		} catch (Throwable t) {
			w(t.getMessage());
		}
	}

	private String join(String[] tokens) {
		StringBuilder sb = new StringBuilder();
		int p = 0;
		for (int i = 1; i < tokens.length; i++) {
			String t = tokens[i];
			if (p++ != 0)
				sb.append(" ");
			sb.append(t);
		}

		return sb.toString();
	}

	private void startQuery(String[] tokens) {
		if (client == null) {
			w("connect first please");
			return;
		}

		if (tokens.length < 2) {
			w("Usage: start_query <query_id>");
			return;
		}

		try {
			int id = Integer.valueOf(tokens[1]);
			client.startQuery(id);
			w("started query " + id);
		} catch (Throwable t) {
			w(t.getMessage());
		}
	}

	private void stopQuery(String[] tokens) {
		if (client == null) {
			w("connect first please");
			return;
		}

		if (tokens.length < 2) {
			w("Usage: stop_query <query_id>");
			return;
		}

		try {
			int id = Integer.valueOf(tokens[1]);
			client.stopQuery(id);
			w("stopped query " + id);
		} catch (Throwable t) {
			w(t.getMessage());
		}
	}

	private void removeQuery(String[] tokens) {
		if (client == null) {
			w("connect first please");
			return;
		}

		if (tokens.length < 2) {
			w("Usage: remove_query <query_id>");
			return;
		}
		try {
			int id = Integer.valueOf(tokens[1]);
			client.removeQuery(id);
			w("removed query " + id);
		} catch (Throwable t) {
			w(t.getMessage());
		}
	}

	@SuppressWarnings("unchecked")
	private void fetch(String[] tokens) {
		if (client == null) {
			w("connect first please");
			return;
		}

		if (tokens.length < 4) {
			w("Usage: fetch <query_id> <offset> <limit>");
			return;
		}

		int id = Integer.valueOf(tokens[1]);
		long offset = Long.valueOf(tokens[2]);
		int limit = Integer.valueOf(tokens[3]);

		try {
			Map<String, Object> page = client.getResult(id, offset, limit);
			List<Object> rows = (List<Object>) page.get("result");
			for (Object row : rows)
				w(row.toString());
			w(rows.size() + " row(s)");
		} catch (Throwable t) {
			w(t.getMessage());
		}
	}

	private void createTable(String[] tokens) {
		if (client == null) {
			w("connect first please");
			return;
		}

		if (tokens.length < 2) {
			w("Usage: create_table <table_name>");
			return;
		}

		try {
			client.createTable(tokens[1]);
		} catch (Throwable t) {
			w(t.getMessage());
		}
	}

	private void dropTable(String[] tokens) {
		if (client == null) {
			w("connect first please");
			return;
		}

		if (tokens.length < 2) {
			w("Usage: drop_table <table_name>");
			return;
		}

		try {
			client.dropTable(tokens[1]);
		} catch (Throwable t) {
			w(t.getMessage());
		}
	}

	private String getPrompt() {
		if (client != null)
			return "logdb@" + host + "> ";
		return "logdb> ";
	}

	private void help() {
		w("connect <host> <loginname> <password>");
		w("\tconnect to specified araqne logdb instance");

		w("disconnect");
		w("\tdisconnect database connection");

		w("queries");
		w("\tprint all queries initiated by this session");

		w("query <query string>");
		w("\tcreate, start and fetch query result at once");

		w("create_query <query string>");
		w("\tcreate query with specified query string, and return allocated query id");

		w("start_query <query id>");
		w("\tstart query");

		w("stop_query <query_id>");
		w("\tstop running query");

		w("remove_query <query_id>");
		w("\tstop and remove query");

		w("fetch <query_id> <offset> <limit>");
		w("\tfetch result set of specified window. you can fetch partial result before query is ended");
	}

	private static void w(String s) {
		System.out.println(s);
	}
}
