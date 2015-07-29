package org.araqne.logdb;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class QueryCommandHelp {
	private String commandName;
	private Map<Locale, String> descriptions = new HashMap<Locale, String>();
	private Map<String, QueryCommandOption> options = new HashMap<String, QueryCommandOption>();
	private Map<Locale, String> usages = new HashMap<Locale, String>();

	public String getCommandName() {
		return commandName;
	}

	public void setCommandName(String commandName) {
		this.commandName = commandName;
	}

	public Map<Locale, String> getDescriptions() {
		return descriptions;
	}

	public void setDescriptions(Map<Locale, String> descriptions) {
		this.descriptions = descriptions;
	}

	public Map<String, QueryCommandOption> getOptions() {
		return options;
	}

	public void setOptions(Map<String, QueryCommandOption> options) {
		this.options = options;
	}

	public Map<Locale, String> getUsages() {
		return usages;
	}

	public void setUsages(Map<Locale, String> usages) {
		this.usages = usages;
	}

	@Override
	public String toString() {
		return "command_name=" + commandName + ", descriptions=" + descriptions + ", options=" + options + ", usages=" + usages;
	}
}
