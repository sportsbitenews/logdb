package org.araqne.logdb;

public class ParserContext {
	private String commandName;
	private int baseOffset;

	public String getCommandName() {
		return commandName;
	}

	public void setCommandName(String commandName) {
		this.commandName = commandName;
	}

	public int getBaseOffset() {
		return baseOffset;
	}

	public void setBaseOffset(int baseOffset) {
		this.baseOffset = baseOffset;
	}

}
