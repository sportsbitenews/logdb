package org.araqne.logdb.jython.impl;

import org.araqne.confdb.CollectionName;

@CollectionName("scripts")
public class ScriptConfig {
	// unique name
	private String name;

	// query, logger, parser
	private String type;

	// script file content
	private String script;

	public ScriptConfig() {
	}

	public ScriptConfig(String name, String type, String script) {
		this.name = name;
		this.type = type;
		this.script = script;
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
