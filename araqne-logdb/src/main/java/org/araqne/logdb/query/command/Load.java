package org.araqne.logdb.query.command;

import java.util.HashMap;
import java.util.Map;

import org.araqne.logdb.LogMap;
import org.araqne.logdb.LogQueryCommand;
import org.araqne.logstorage.Log;
import org.araqne.logstorage.LogCursor;

/**
 * load saved query result
 * 
 * @author xeraph
 * 
 */
public class Load extends LogQueryCommand {

	private LogCursor cursor;

	public Load(LogCursor cursor) {
		this.cursor = cursor;
	}

	@Override
	public void start() {
		boolean cancelled = false;
		try {
			status = Status.Running;

			while (cursor.hasNext()) {
				Status status = getStatus();
				if (status == Status.End)
					break;

				Log log = cursor.next();
				Map<String, Object> m = new HashMap<String, Object>();
				m.putAll(log.getData());
				m.put("_time", log.getDate());
				m.put("_id", log.getId());
				write(new LogMap(m));
			}
		} catch (Throwable t) {
			cancelled = true;
		} finally {
			cursor.close();
			eof(cancelled);
		}
	}

	@Override
	public void push(LogMap m) {
	}

	@Override
	public boolean isReducer() {
		return false;
	}

}
