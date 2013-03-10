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
package org.araqne.logdb.query.command;

import java.util.Map;

import org.araqne.logdb.LogMap;
import org.araqne.logdb.LogQueryCommand;
import org.araqne.logdb.LogQueryScript;
import org.araqne.logdb.LogQueryScriptInput;
import org.araqne.logdb.LogQueryScriptOutput;
import org.osgi.framework.BundleContext;

public class Script extends LogQueryCommand {
	private BundleContext bc;
	private String scriptName;
	private Map<String, String> params;
	private LogQueryScript script;
	private DefaultScriptInput input;
	private DefaultScriptOutput output;

	public Script(BundleContext bc, String scriptName, Map<String, String> params, LogQueryScript script) {
		this.bc = bc;
		this.scriptName = scriptName;
		this.params = params;
		this.script = script;
		this.input = new DefaultScriptInput();
		this.output = new DefaultScriptOutput();
	}

	public String getScriptName() {
		return scriptName;
	}

	public Map<String, String> getParameters() {
		return params;
	}

	public LogQueryScript getQueryScript() {
		return script;
	}

	@Override
	public void push(LogMap m) {
		input.data = m.map();
		script.handle(input, output);
	}

	@Override
	public boolean isReducer() {
		return true;
	}

	@Override
	public void eof() {
		this.status = Status.Finalizing;
		script.eof(output);
		super.eof();
	}

	private void out(LogMap data) {
		write(data);
	}

	private class DefaultScriptInput implements LogQueryScriptInput {
		private Map<String, Object> data;

		@Override
		public BundleContext getBundleContext() {
			return bc;
		}

		@Override
		public Map<String, Object> getData() {
			return data;
		}
	}

	private class DefaultScriptOutput implements LogQueryScriptOutput {
		@Override
		public void write(Map<String, Object> data) {
			out(new LogMap(data));
		}
	}
}
