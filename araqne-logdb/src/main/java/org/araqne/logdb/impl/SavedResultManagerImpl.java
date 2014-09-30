package org.araqne.logdb.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
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
import org.araqne.confdb.Predicate;
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
import org.araqne.storage.api.FilePath;
import org.araqne.storage.api.StorageInputStream;
import org.araqne.storage.api.StorageOutputStream;
import org.araqne.storage.localfile.LocalFilePath;
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

	private FilePath baseDir;

	@Validate
	public void start() {
		baseDir = new LocalFilePath(System.getProperty("araqne.data.dir")).newFilePath("araqne-logdb/saved");
		baseDir.mkdirs();
	}

	@Override
	public List<SavedResult> getResultList(String owner) {
		ConfigDatabase db = conf.ensureDatabase("araqne-logdb");
		Predicate pred = null;
		if (owner != null)
			pred = Predicates.field("owner", owner);

		ConfigIterator it = db.find(SavedResult.class, pred);
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

		FilePath fromIndexPath = new LocalFilePath(result.getIndexPath());
		FilePath toIndexPath = baseDir.newFilePath(result.getGuid() + ".idx");

		FilePath fromDataPath = new LocalFilePath(result.getDataPath());
		FilePath toDataPath = baseDir.newFilePath(result.getGuid() + ".dat");

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

	private void copy(FilePath from, FilePath to) throws IOException {
		StorageInputStream src = null;
		StorageOutputStream dst = null;
		try {
			src = from.newInputStream();
			dst = to.newOutputStream(false);

			long length = src.length();
			long copied = 0;
			ByteBuffer bb = ByteBuffer.allocate(8192);
			while (true) {
				int read = src.read(bb.array(), 0, bb.limit());
				dst.write(bb.array(), 0, read);
				copied += read;
				if (read != bb.capacity())
					// eof
					break;
			}
			
			if (copied != length)
				throw new IOException("copied size is not equal with length of source file");
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

		FilePath indexPath = baseDir.newFilePath(guid + ".idx");
		FilePath dataPath = baseDir.newFilePath(guid + ".dat");

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

		LogFileService lfs = fileServiceRegistry.getLogFileService(sr.getStorageName());
		Map<String, Object> options = new HashMap<String, Object>();
		options.put("indexPath", baseDir.newFilePath(guid + ".idx"));
		options.put("dataPath", baseDir.newFilePath(guid + ".dat"));

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
