package org.araqne.logdb.query.command;

import java.util.HashMap;
import java.util.Map;

import org.araqne.logdb.Row;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.RowPipe;
import org.araqne.logdb.query.engine.QueryTask;
import org.araqne.logstorage.Log;
import org.araqne.logstorage.LogCursor;

/**
 * load saved query result
 * 
 * @author xeraph
 * 
 */
public class Load extends QueryCommand {
	private LoadTask mainTask = new LoadTask();
	private LogCursor cursor;
	private String guid;

	public Load(LogCursor cursor, String guid) {
		this.cursor = cursor;
		this.guid = guid;
	}

	@Override
	public QueryTask getMainTask() {
		return mainTask;
	}

	@Override
	public String toString() {
		return "load " + guid;
	}

	private class LoadTask extends QueryTask {
		@Override
		public void run() {
			try {
				status = Status.Running;

				while (cursor.hasNext()) {
					TaskStatus status = getStatus();
					if (status == TaskStatus.CANCELED)
						break;

					Log log = cursor.next();
					Map<String, Object> m = new HashMap<String, Object>();
					m.putAll(log.getData());
					m.put("_time", log.getDate());
					m.put("_id", log.getId());
					pushPipe(new Row(m));
				}
			} finally {
				cursor.close();
			}
		}

		@Override
		public RowPipe getOutput() {
			return output;
		}
	}
}
