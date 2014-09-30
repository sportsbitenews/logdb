package org.araqne.logdb.query.command;

import java.util.List;
import java.util.Map;

import org.araqne.log.api.Log;
import org.araqne.log.api.LogPipe;
import org.araqne.log.api.LoggerRegistry;
import org.araqne.logdb.DriverQueryCommand;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.Strings;
import org.araqne.logdb.TimeSpan;

public class Logger extends DriverQueryCommand implements LogPipe {

	private LoggerRegistry loggerRegistry;
	private TimeSpan window;
	private List<String> loggerNames;

	private volatile boolean stopped = false;

	public Logger(LoggerRegistry loggerRegistry, TimeSpan window, List<String> loggerNames) {
		this.loggerRegistry = loggerRegistry;
		this.window = window;
		this.loggerNames = loggerNames;
	}

	@Override
	public String getName() {
		return "logger";
	}

	public void run() {
		try {
			for (String name : loggerNames) {
				org.araqne.log.api.Logger logger = loggerRegistry.getLogger(name);
				if (logger != null)
					logger.addLogPipe(this);
			}

			long expire = System.currentTimeMillis() + window.amount * window.unit.getMillis();

			while (true) {
				if (System.currentTimeMillis() >= expire)
					break;

				if (stopped)
					break;

				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
				}
			}
		} finally {
			for (String name : loggerNames) {
				org.araqne.log.api.Logger logger = loggerRegistry.getLogger(name);
				if (logger != null)
					logger.removeLogPipe(this);
			}
		}
	}

	@Override
	public void onLog(org.araqne.log.api.Logger logger, Log log) {
		Map<String, Object> m = Row.clone(log.getParams());
		m.put("_logger", logger.getFullName());
		m.put("_time", log.getDate());
		pushPipe(new Row(m));
	}

	@Override
	public void onLogBatch(org.araqne.log.api.Logger logger, Log[] logs) {
		int count = 0;
		for (Log log : logs) {
			if (log != null)
				count++;
		}

		Row[] rows = new Row[count];

		int i = 0;
		for (Log log : logs) {
			if (log != null) {
				Map<String, Object> m = Row.clone(log.getParams());
				m.put("_logger", logger.getFullName());
				m.put("_time", log.getDate());
				rows[i++] = new Row(m);
			}
		}

		RowBatch rowBatch = new RowBatch();
		rowBatch.rows = rows;
		rowBatch.size = count;

		pushPipe(rowBatch);
	}

	@Override
	public void onClose(QueryStopReason reason) {
		stopped = true;
	}

	@Override
	public String toString() {
		return "logger window=" + window + " " + Strings.join(loggerNames, ", ");
	}
}
