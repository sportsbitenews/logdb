package org.araqne.logdb;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class RowBatchPacker extends TimerTask {
	private int flushThreshold;
	private long flushInterval;
	private RowPipe output;
	private Timer timer;
	private List<Row> pack;

	public RowBatchPacker(String name, int threshold, int interval, RowPipe output) {
		this.flushThreshold = threshold;
		this.flushInterval = interval;
		this.output = output;
		this.pack = new ArrayList<Row>(threshold);
		this.timer = new Timer("Row Batch Packer [" + name + "]", true);
		this.timer.schedule(this, 0, flushInterval);
	}

	public void pack(Row row) {
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
		RowBatch rowBatch = new RowBatch();
		synchronized (pack) {
			if (pack.isEmpty())
				return;
			
			rowBatch.size = pack.size();
			rowBatch.rows = pack.toArray(new Row[0]);
		}
		output.onRowBatch(rowBatch);
	}

	public void close() {
		flush();
		timer.cancel();
	}
}
