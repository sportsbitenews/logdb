package org.araqne.logstorage.dump.engine;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.araqne.codec.FastEncodingRule;
import org.araqne.logstorage.Log;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogTraverseCallback;
import org.araqne.logstorage.LogTraverseCallback.Sink;
import org.araqne.logstorage.TableScanRequest;
import org.araqne.logstorage.dump.DumpService;
import org.araqne.logstorage.dump.ExportRequest;
import org.araqne.logstorage.dump.ExportTableKey;
import org.araqne.logstorage.dump.ExportTabletTask;
import org.araqne.logstorage.dump.ExportTask;
import org.araqne.logstorage.dump.ExportWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalExportWorker implements ExportWorker {
	private final Logger slog = LoggerFactory.getLogger(LocalExportWorker.class);

	private ExportRequest req;
	private ExportTask task;

	private DumpService dumpService;
	private LogStorage storage;
	private FileOutputStream fos;
	private ZipOutputStream zos;
	private BufferedOutputStream bos;

	public LocalExportWorker(ExportRequest req, DumpService dumpService, LogStorage storage) {
		this.req = req;
		this.dumpService = dumpService;
		this.storage = storage;
		this.task = new ExportTask(req.getGuid());
	}

	@Override
	public ExportTask getTask() {
		return task;
	}

	@Override
	public void run() {
		slog.info("araqne logstorage: start export estimation [{}]", req.getGuid());
		List<ExportTabletTask> tabletTasks = dumpService.estimate(req);
		Map<ExportTableKey, ExportTabletTask> m = task.getTabletTasks();
		for (ExportTabletTask t : tabletTasks)
			m.put(new ExportTableKey(t.getTableName(), t.getDay()), t);

		slog.info("araqne logstorage: start export job [{}]", req.getGuid());

		try {
			String path = req.getParams().get("path");

			fos = new FileOutputStream(new File(path));
			zos = new ZipOutputStream(fos);
			bos = new BufferedOutputStream(zos, 8192 * 4);

			String lastTableName = null;
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			for (ExportTabletTask task : tabletTasks) {
				if (lastTableName == null || !lastTableName.equals(task.getTableName())) {
					zos.putNextEntry(new ZipEntry(task.getTableName() + "/"));
					lastTableName = task.getTableName();
				}

				String entryPath = task.getTableName() + "/" + df.format(task.getDay()) + ".dmp";
				zos.putNextEntry(new ZipEntry(entryPath));

				OutputLoader loader = new OutputLoader(new OutputSink(0, 0), task);

				TableScanRequest scanReq = new TableScanRequest();
				scanReq.setTableName(task.getTableName());
				scanReq.setFrom(task.getDay());
				scanReq.setTo(nextDay(task.getDay()));
				scanReq.setUseSerialScan(true);
				scanReq.setAsc(true);
				scanReq.setTraverseCallback(loader);

				try {
					storage.search(scanReq);
				} catch (InterruptedException e) {
				}

				task.setActualCount(loader.count);
				task.setCompleted(true);

				zos.closeEntry();
			}
		} catch (Throwable t) {
			slog.error("araqne logstorage: export failed", t);
		} finally {
			ensureClose(bos);
			ensureClose(zos);
			ensureClose(fos);
			task.setCompleted();
			slog.info("araqne logstorage: export completed");
		}
	}

	private Date nextDay(Date d) {
		Calendar c = Calendar.getInstance();
		c.add(Calendar.DAY_OF_MONTH, 1);
		c.set(Calendar.HOUR_OF_DAY, 0);
		c.set(Calendar.MINUTE, 0);
		c.set(Calendar.SECOND, 0);
		c.set(Calendar.MILLISECOND, 0);
		return c.getTime();
	}

	private void ensureClose(Closeable c) {
		try {
			c.close();
		} catch (IOException e) {
		}
	}

	private class OutputSink extends Sink {
		public OutputSink(long offset, long limit) {
			super(offset, limit);
		}

		@Override
		protected void processLogs(List<Log> logs) {
			FastEncodingRule enc = new FastEncodingRule();
			for (Log log : logs) {
				Map<String, Object> data = log.getData();
				data.put("_time", log.getDate());

				try {
					ByteBuffer bb = enc.encode(data);
					bos.write(bb.array());
				} catch (IOException e) {
					slog.error("araqne logstorage: output failure", e);
				}
			}
		}
	}

	private class OutputLoader extends LogTraverseCallback {
		private long count = 0;
		private ExportTabletTask task;

		public OutputLoader(Sink sink, ExportTabletTask task) {
			super(sink);
			this.task = task;
		}

		private volatile boolean doStop;

		@Override
		public void interrupt() {
			doStop = true;
		}

		@Override
		public boolean isInterrupted() {
			return doStop;
		}

		@Override
		protected List<Log> filter(List<Log> logs) {
			count += logs.size();
			task.setActualCount(count);
			return logs;
		}
	}
}
