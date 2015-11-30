package org.araqne.logdb.query.command;

import java.util.ArrayList;
import java.util.List;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;

public class MakeBatch extends QueryCommand {
	private long lastTime;
	private List<Row> rows;

	public MakeBatch() {
		this.lastTime = System.currentTimeMillis();
		this.rows = new ArrayList<Row>();
	}

	@Override
	public void onClose(QueryStopReason reason) {
		pushRowBatch();
	}

	@Override
	public String getName() {
		return "makebatch";
	}

	@Override
	public void onPush(Row row) {
		rows.add(row);

		long now = System.currentTimeMillis();
		if (now - lastTime > 100) {
			pushRowBatch();
			lastTime = System.currentTimeMillis();
		}
	}

	private void pushRowBatch() {
		if (rows.size() > 0) {
			RowBatch batch = new RowBatch();
			batch.size = rows.size();
			batch.rows = rows.toArray(new Row[rows.size()]);
			pushPipe(batch);
			rows.clear();
		}
	}

	@Override
	public void onPush(RowBatch rowBatch) {
		if (rowBatch.selectedInUse) {
			for (int i = 0; i < rowBatch.size; i++) {
				Row row = rowBatch.rows[rowBatch.selected[i]];
				rows.add(row);
			}
		} else {
			for (int i = 0; i < rowBatch.size; i++) {
				Row row = rowBatch.rows[i];
				rows.add(row);
			}
		}

		long now = System.currentTimeMillis();
		if (now - lastTime > 100) {
			pushRowBatch();
			lastTime = System.currentTimeMillis();
		}
	}

	@Override
	public String toString() {
		return "makebatch";
	}
}
