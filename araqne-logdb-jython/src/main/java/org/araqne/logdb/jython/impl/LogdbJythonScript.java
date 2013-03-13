/*
 * Copyright 2012 Future Systems
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
package org.araqne.logdb.jython.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.araqne.api.Primitive;
import org.araqne.api.Script;
import org.araqne.api.ScriptArgument;
import org.araqne.api.ScriptContext;
import org.araqne.api.ScriptUsage;
import org.araqne.logdb.LogQueryScript;
import org.araqne.logdb.LogQueryScriptInput;
import org.araqne.logdb.LogQueryScriptOutput;
import org.araqne.logdb.jython.JythonLoggerScriptRegistry;
import org.araqne.logdb.jython.JythonQueryScriptRegistry;
import org.osgi.framework.BundleContext;

public class LogdbJythonScript implements Script {
	private JythonQueryScriptRegistry queryScriptRegistry;
	private JythonLoggerScriptRegistry loggerScriptRegistry;
	private BundleContext bc;
	private ScriptContext context;

	public LogdbJythonScript(BundleContext bc, JythonQueryScriptRegistry queryScriptRegistry,
			JythonLoggerScriptRegistry loggerScriptRegistry) {
		this.bc = bc;
		this.queryScriptRegistry = queryScriptRegistry;
		this.loggerScriptRegistry = loggerScriptRegistry;
	}

	@Override
	public void setScriptContext(ScriptContext context) {
		this.context = context;
	}

	public void loggerScripts(String[] args) {
		context.println("Logger Scripts");
		context.println("----------------");
		for (String name : loggerScriptRegistry.getScriptNames()) {
			context.println(name);
		}
	}

	public void queryScripts(String[] args) {
		context.println("Query Scripts");
		context.println("---------------");
		for (String workspace : queryScriptRegistry.getWorkspaceNames()) {
			context.println("Workspace: " + workspace);
			for (String name : queryScriptRegistry.getScriptNames(workspace)) {
				context.println("  " + name);
			}
		}
	}

	@ScriptUsage(description = "print logger script", arguments = { @ScriptArgument(name = "script name", type = "string", description = "script name") })
	public void loggerScript(String[] args) {
		String s = loggerScriptRegistry.getScriptCode(args[0]);
		if (s == null) {
			context.println("logger script not found");
			return;
		}

		context.println(s);
	}

	@ScriptUsage(description = "print script", arguments = {
			@ScriptArgument(name = "workspace name", type = "string", description = "workspace name"),
			@ScriptArgument(name = "script name", type = "string", description = "script name") })
	public void queryScript(String[] args) {
		String s = queryScriptRegistry.getScriptCode(args[0], args[1]);
		if (s == null) {
			context.println("query script not found");
			return;
		}

		context.println(s);
	}

	@ScriptUsage(description = "run query script for test", arguments = {
			@ScriptArgument(name = "workspace name", type = "string", description = "workspace name"),
			@ScriptArgument(name = "script name", type = "string", description = "script name"),
			@ScriptArgument(name = "line", type = "string", description = "test data") })
	public void runQueryScript(String[] args) {
		LogQueryScript s = queryScriptRegistry.newLogScript(args[0], args[1], null);
		if (s == null) {
			context.println("script not found");
			return;
		}

		s.handle(new ConsoleInput(args[2]), new ConsoleOutput());
	}

	private class ConsoleInput implements LogQueryScriptInput {
		private Map<String, Object> data;

		public ConsoleInput(String line) {
			data = new HashMap<String, Object>();
			data.put("line", line);
		}

		@Override
		public BundleContext getBundleContext() {
			return bc;
		}

		@Override
		public Map<String, Object> getData() {
			return data;
		}
	}

	private class ConsoleOutput implements LogQueryScriptOutput {
		@Override
		public void write(Map<String, Object> data) {
			context.println(Primitive.stringify(data));
		}
	}

	@ScriptUsage(description = "import logger script file", arguments = {
			@ScriptArgument(name = "script name", type = "string", description = "jython class name"),
			@ScriptArgument(name = "file path", type = "string", description = "absolute or relative script file path") })
	public void loadLoggerScript(String[] args) {
		File dir = (File) context.getSession().getProperty("dir");
		File f = canonicalize(dir, args[1]);
		try {
			String s = readAllLines(f);
			loggerScriptRegistry.loadScript(args[0], s);
			context.println("loaded " + countLines(s) + " lines");
		} catch (FileNotFoundException e) {
			context.println("file not found: " + f.getAbsolutePath());
		} catch (IOException e) {
			context.println(e.getMessage());
		}
	}

	@ScriptUsage(description = "import query script file", arguments = {
			@ScriptArgument(name = "workspace name", type = "string", description = "workspace name"),
			@ScriptArgument(name = "script name", type = "string", description = "jython class name"),
			@ScriptArgument(name = "file path", type = "string", description = "absolute or relative script file path") })
	public void loadQueryScript(String[] args) {
		File dir = (File) context.getSession().getProperty("dir");
		File f = canonicalize(dir, args[2]);
		try {
			String s = readAllLines(f);
			queryScriptRegistry.setScript(args[0], args[1], s);
			context.println("loaded " + countLines(s) + " lines");
		} catch (FileNotFoundException e) {
			context.println("file not found: " + f.getAbsolutePath());
		} catch (IOException e) {
			context.println(e.getMessage());
		}
	}

	@ScriptUsage(description = "unload logger script", arguments = { @ScriptArgument(name = "script name", type = "string", description = "jython class name") })
	public void unloadLoggerScript(String[] args) {
		loggerScriptRegistry.unloadScript(args[0]);
		context.println("unloaded");
	}

	@ScriptUsage(description = "unload query script", arguments = {
			@ScriptArgument(name = "workspace name", type = "string", description = "workspace name"),
			@ScriptArgument(name = "script name", type = "string", description = "jython class name") })
	public void unloadQueryScript(String[] args) {
		queryScriptRegistry.removeScript(args[0], args[1]);
		context.println("unloaded");
	}

	private static int countLines(String s) {
		int last = 0;
		int count = 0;
		while (true) {
			last = s.indexOf('\n', last);
			if (last < 0)
				break;

			last = last + 1;
			count++;
		}
		return count;
	}

	private String readAllLines(File f) throws IOException {
		StringBuilder sb = new StringBuilder();
		FileInputStream is = new FileInputStream(f);
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(is));
			while (true) {
				String line = br.readLine();
				if (line == null)
					break;

				context.println(line);
				sb.append(line);
				sb.append("\n");
			}

			return sb.toString();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
				}
			}

			if (is != null)
				is.close();
		}
	}

	private File canonicalize(File dir, String path) {
		if (path.startsWith("/"))
			return new File(path);
		else
			return new File(dir, path);
	}
}
