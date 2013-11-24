package org.araqne.logdb;

import java.io.IOException;
import java.util.Date;

public interface QueryResult extends RowPipe {
	Date getEofDate();

	long getCount();

	void syncWriter() throws IOException;

	void closeWriter();

	void purge();
}
