package org.araqne.logdb.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.confdb.Config;
import org.araqne.confdb.ConfigDatabase;
import org.araqne.confdb.ConfigIterator;
import org.araqne.confdb.ConfigService;
import org.araqne.confdb.Predicates;
import org.araqne.logdb.SavedResult;
import org.araqne.logdb.SavedResultManager;
import org.araqne.logstorage.Log;
import org.araqne.logstorage.LogCursor;
import org.araqne.logstorage.LogFileService;
import org.araqne.logstorage.LogFileServiceRegistry;
import org.araqne.logstorage.LogMarshaler;
import org.araqne.logstorage.file.LogFileReader;
import org.araqne.logstorage.file.LogRecord;
import org.araqne.logstorage.file.LogRecordCursor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name = "saved-result-manager")
@Provides
public class SavedResultManagerImpl implements SavedResultManager {
	private final Logger logger = LoggerFactory.getLogger(SavedResultManagerImpl.class);

	@Requires
	private ConfigService conf;

	@Requires
	private LogFileServiceRegistry fileServiceRegistry;

	private File baseDir;

	@Validate
	public void start() {
		baseDir = new File(System.getProperty("araqne.data.dir"), "araqne-logdb/saved");
		baseDir.mkdirs();
	}

	@Override
	public List<SavedResult> getResultList(String owner) {
		ConfigDatabase db = conf.ensureDatabase("araqne-logdb");
		ConfigIterator it = db.find(SavedResult.class, null);
		return new ArrayList<SavedResult>(it.getDocuments(SavedResult.class));
	}

	@Override
	public SavedResult getResult(String guid) {
		ConfigDatabase db = conf.ensureDatabase("araqne-logdb");
		Config c = db.findOne(SavedResult.class, Predicates.field("guid", guid));
		if (c != null)
			return c.getDocument(SavedResult.class);

		return null;
	}

	@Override
	public void saveResult(SavedResult result) throws IOException {
		ConfigDatabase db = conf.ensureDatabase("araqne-logdb");
		Config c = db.findOne(SavedResult.class, Predicates.field("guid", result.getGuid()));
		if (c != null)
			throw new IllegalStateException("duplicated guid of saved result: " + result.getGuid());

		File fromIndexPath = new File(result.getIndexPath());
		File toIndexPath = new File(baseDir, result.getGuid() + ".idx");

		File fromDataPath = new File(result.getDataPath());
		File toDataPath = new File(baseDir, result.getGuid() + ".dat");

		try {
			copy(fromIndexPath, toIndexPath);
			copy(fromDataPath, toDataPath);
			db.add(result);
		} catch (IOException e) {
			toIndexPath.delete();
			toDataPath.delete();
			throw e;
		}
	}

	private void copy(File from, File to) throws IOException {
		FileInputStream src = null;
		FileOutputStream dst = null;
		try {
			src = new FileInputStream(from);
			dst = new FileOutputStream(to);

			FileChannel srcChannel = src.getChannel();
			FileChannel dstChannel = dst.getChannel();
			long size = srcChannel.size();
			srcChannel.transferTo(0, size, dstChannel);
		} finally {
			if (src != null) {
				try {
					src.close();
				} catch (IOException e) {
				}
			}

			if (dst != null) {
				try {
					dst.close();
				} catch (IOException e) {
				}
			}
		}
	}

	@Override
	public void deleteResult(String guid) {
		ConfigDatabase db = conf.ensureDatabase("araqne-logdb");
		Config c = db.findOne(SavedResult.class, Predicates.field("guid", guid));
		if (c == null)
			throw new IllegalStateException("saved result not found: " + guid);

		c.remove();

		File indexPath = new File(baseDir, guid + ".idx");
		File dataPath = new File(baseDir, guid + ".dat");

		if (!indexPath.delete())
			logger.error("araqne logdb: cannot delete saved result [{}]", indexPath.getAbsolutePath());
		if (!dataPath.delete())
			logger.error("araqne logdb: cannot delete saved result [{}]", dataPath.getAbsolutePath());
	}

	@Override
	public LogCursor getCursor(String guid) throws IOException {
		SavedResult sr = getResult(guid);
		if (sr == null)
			throw new IllegalStateException("saved result not found: " + guid);

		LogFileService lfs = fileServiceRegistry.getLogFileService(sr.getType());
		Map<String, Object> options = new HashMap<String, Object>();
		options.put("indexPath", new File(baseDir, guid + ".idx"));
		options.put("dataPath", new File(baseDir, guid + ".dat"));

		LogFileReader reader = lfs.newReader("result-" + guid, options);
		return new LogCursorImpl(sr, reader);
	}

	private static class LogCursorImpl implements LogCursor {
		private String guid;
		private LogFileReader reader;
		private LogRecordCursor c;

		public LogCursorImpl(SavedResult result, LogFileReader reader) throws IOException {
			this.guid = result.getGuid();
			this.reader = reader;
			this.c = reader.getCursor();
		}

		@Override
		public boolean hasNext() {
			return c.hasNext();
		}

		@Override
		public Log next() {
			LogRecord next = c.next();
			return LogMarshaler.convert(guid, next);
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		public void close() {
			reader.close();
		}
	}
}
