package org.araqne.logdb;

import java.util.List;

public interface MultiSubQueryCommand extends SubQueryCommand {
	List<Query> getSubQueries();
}
