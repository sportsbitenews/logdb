package org.araqne.logstorage.dump.engine;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.araqne.api.Io;
import org.araqne.codec.EncodingRule;
import org.araqne.logstorage.Log;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.dump.DumpManifest;
import org.araqne.logstorage.dump.DumpService;
import org.araqne.logstorage.dump.DumpTabletEntry;
import org.araqne.logstorage.dump.DumpTabletKey;
import org.araqne.logstorage.dump.ImportRequest;
import org.araqne.logstorage.dump.ImportTabletTask;
import org.araqne.logstorage.dump.ImportTask;
import org.araqne.logstorage.dump.ImportWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalImportWorker implements ImportWorker {
	private final Logger slog = LoggerFactory.getLogger(LocalImportWorker.class);

	private ImportRequest req;
	private ImportTask task;

	private DumpService dumpService;
	private LogStorage storage;

	private File path;

	public LocalImportWorker(ImportRequest req, DumpService dumpService, LogStorage storage) {
		this.req = req;
		this.dumpService = dumpService;
		this.storage = storage;
		this.task = new ImportTask(req);
		for (DumpTabletEntry e : req.getEntries()) {
			DumpTabletKey key = new DumpTabletKey(e.getTableName(), e.getDay());
			ImportTabletTask val = new ImportTabletTask(e.getTableName(), e.getDay(), e.getCount());
			task.getTabletTasks().put(key, val);
		}

		this.path = new File(req.getParams().get("path"));
	}

	@Override
	public ImportTask getTask() {
		return task;
	}

	@Override
	public void run() {
		ZipFile zipFile = null;
		try {
			DumpManifest manifest = dumpService.readManifest("local", req.getParams());
			Map<String, Integer> tables = manifest.getTables();

			Set<DumpTabletKey> keys = new HashSet<DumpTabletKey>();
			for (DumpTabletEntry e : manifest.getEntries()) {
				keys.add(new DumpTabletKey(e.getTableName(), e.getDay()));
			}

			zipFile = new ZipFile(path);

			for (DumpTabletKey key : task.getTabletTasks().keySet()) {
				if (task.isCancelled())
					return;

				ImportTabletTask e = task.getTabletTasks().get(key);
				if (!keys.contains(key))
					continue;

				Integer tableId = tables.get(e.getTableName());
				if (tableId == null)
					continue;

				loadFile(zipFile, e, tableId);
			}

		} catch (IOException e) {
			slog.error("araqne logstorage: import failed", e);
		} catch (InterruptedException e) {
		} finally {
			if (zipFile != null) {
				try {
					zipFile.close();
				} catch (IOException e) {
				}
			}

			slog.info("araqne logstorage: import completed");
		}
	}

	@SuppressWarnings("unchecked")
	private void loadFile(ZipFile zipFile, ImportTabletTask dumpEntry, int tableId) throws IOException, InterruptedException {
		long total = 0;
		InputStream is = null;
		try {
			String tableName = dumpEntry.getTableName();
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			ZipEntry zipEntry = new ZipEntry(tableId + "/" + df.format(dumpEntry.getDay()) + ".dmp");
			is = zipFile.getInputStream(zipEntry);

			while (true) {
				byte[] blen = new byte[4];
				int readBytes = Io.ensureRead(is, blen, 4);
				if (readBytes <= 0)
					break;

				int len = Io.decodeInt(blen);

				byte[] b = new byte[len];
				readBytes = Io.ensureRead(is, b, len);
				if (readBytes <= 0)
					break;

				Object[] arr = (Object[]) EncodingRule.decode(ByteBuffer.wrap(b));
				List<Log> logs = new ArrayList<Log>();

				for (Object o : arr) {
					Map<String, Object> m = (Map<String, Object>) o;
					Date d = (Date) m.get("_time");
					Log log = new Log(tableName, d, m);
					logs.add(log);
				}

				total += logs.size();

				storage.write(logs);
				dumpEntry.setImportCount(total);
			}
		} catch (EOFException ex) {
		} finally {
			if (is != null) {
				is.close();
			}

			dumpEntry.setCompleted(true);
		}
	}
}
