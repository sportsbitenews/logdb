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
package org.araqne.logdb.jython.impl;

import org.araqne.confdb.CollectionName;

@CollectionName("scripts")
public class ScriptConfig {
	// required for query script only
	private String workspace;

	// unique name
	private String name;

	// query, logger, parser
	private String type;

	// script file content
	private String script;

	public ScriptConfig() {
	}

	public ScriptConfig(String name, String type, String script) {
		this(null, name, type, script);
	}

	public ScriptConfig(String workspace, String name, String type, String script) {
		this.workspace = workspace;
		this.name = name;
		this.type = type;
		this.script = script;
	}

	public String getWorkspace() {
		return workspace;
	}

	public void setWorkspace(String workspace) {
		this.workspace = workspace;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getScript() {
		return script;
	}

	public void setScript(String script) {
		this.script = script;
	}
}
