package org.araqne.logdb.query.command;

import org.araqne.logdb.DriverQueryCommand;
public class EmptyCommand extends DriverQueryCommand  {

	@Override
	public void run() {
		//DoNothing.
	}

	@Override
	public String getName() {
		return "empty";
	}
}
