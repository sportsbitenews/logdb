package org.araqne.logdb.query.command;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;

public class Bypass extends QueryCommand {

	@Override
	public String getName() {
		return "bypass";
	}

	@Override
	public void onPush(Row row) {
		pushPipe(row);
	}

	@Override
	public void onPush(RowBatch rowBatch) {
		pushPipe(rowBatch);
	}

	@Override
	public String toString() {
		return "bypass";
	}
}
