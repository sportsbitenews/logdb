package org.araqne.logdb.query.command;

import org.araqne.logdb.DriverQueryCommand;
import org.araqne.logdb.QueryTask.TaskStatus;
import org.araqne.logdb.Row;
import org.araqne.logstorage.Log;
import org.araqne.logstorage.LogCursor;

/**
 * load saved query result
 * 
 * @author xeraph
 * 
 */
public class Load extends DriverQueryCommand {
	private LogCursor cursor;
	private String guid;

	public Load(LogCursor cursor, String guid) {
		this.cursor = cursor;
		this.guid = guid;
	}

	@Override
	public String getName() {
		return "load";
	}

	@Override
	public void run() {
		try {
			while (cursor.hasNext()) {
				TaskStatus status = task.getStatus();
				if (status == TaskStatus.CANCELED)
					break;

				Log log = cursor.next();
				pushPipe(new Row(log.getData()));
			}
		} finally {
			cursor.close();
		}
	}

	@Override
	public String toString() {
		return "load " + guid;
	}
}
