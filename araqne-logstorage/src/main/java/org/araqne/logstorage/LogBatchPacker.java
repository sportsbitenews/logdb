package org.araqne.logstorage;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogBatchPacker extends TimerTask {
	private final Logger slog = LoggerFactory.getLogger(LogBatchPacker.class);

	private String tableName;
	private int flushThreshold;
	private long flushInterval;
	private LogCallback output;
	private Timer timer;
	private List<Log> pack;

	public LogBatchPacker(String tableName, int threshold, int interval, LogCallback output) {
		this.tableName = tableName;
		this.flushThreshold = threshold;
		this.flushInterval = interval;
		this.output = output;
		this.pack = new ArrayList<Log>(threshold);
		this.timer = new Timer("Log Batch Packer [" + tableName + "]", true);
		this.timer.schedule(this, 0, flushInterval);
	}

	public void pack(Log row) {
		synchronized (pack) {
			pack.add(row);

			if (pack.size() >= flushThreshold)
				flush();
		}
	}

	@Override
	public void run() {
		flush();
	}

	public void flush() {
		List<Log> logBatch = null;
		synchronized (pack) {
			if (pack.isEmpty())
				return;

			logBatch = new ArrayList<Log>(pack);
			pack.clear();
		}

		if (slog.isDebugEnabled())
			slog.debug("araqne logstorage: flush log pack, table [{}] count [{}]", tableName, logBatch.size());

		output.onLogBatch(tableName, logBatch);
	}

	public void close() {
		flush();
		timer.cancel();
	}
}
