package org.araqne.logdb;

import java.util.List;

public interface LogQueryPlanner {
	/**
	 * planner may throw parse exception
	 * 
	 * @return the new query execution pipeline
	 */
	List<LogQueryCommand> plan(List<LogQueryCommand> commands);
}
