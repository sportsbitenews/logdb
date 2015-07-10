package org.araqne.logdb.impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.confdb.ConfigDatabase;
import org.araqne.confdb.ConfigIterator;
import org.araqne.confdb.ConfigService;
import org.araqne.logdb.SavedResult;
import org.araqne.logdb.SavedResultManager;
import org.araqne.logdb.query.command.IoHelper;
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
import org.json.JSONConverter;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name = "saved-result-manager")
@Provides
public class SavedResultManagerImpl implements SavedResultManager {
	private static final String FILE_NAME = "saved_results.json";

	private final Logger slog = LoggerFactory.getLogger(SavedResultManagerImpl.class);

	@Requires
	private ConfigService conf;

	@Requires
	private LogFileServiceRegistry fileServiceRegistry;

	private FilePath baseDir;

	// guid to saved result
	private ConcurrentHashMap<String, SavedResult> cachedResults = new ConcurrentHashMap<String, SavedResult>();

	@Validate
	public void start() {
		baseDir = new LocalFilePath(System.getProperty("araqne.data.dir")).newFilePath("araqne-logdb/saved");
		baseDir.mkdirs();
		cachedResults = readSavedResults();

		// migrate
		if (cachedResults == null) {
			cachedResults = new ConcurrentHashMap<String, SavedResult>();
			ConfigDatabase db = conf.ensureDatabase("araqne-logdb");
			ConfigIterator it = db.findAll(SavedResult.class);
			Collection<SavedResult> docs = it.getDocuments(SavedResult.class);
			for (SavedResult doc : docs)
				cachedResults.put(doc.getGuid(), doc);

			writeSaveResult();
		}
	}

	@Override
	public List<SavedResult> getResultList(String owner) {
		List<SavedResult> results = new ArrayList<SavedResult>();
		for (SavedResult result : cachedResults.values())
			if (result.getOwner().equals(owner))
				results.add(result);

		Collections.sort(results);
		return results;
	}

	@Override
	public SavedResult getResult(String guid) {
		return cachedResults.get(guid);
	}

	@Override
	public void saveResult(SavedResult result) throws IOException {
		SavedResult old = cachedResults.putIfAbsent(result.getGuid(), result);
		if (old != null)
			throw new IllegalStateException("duplicated guid of saved result: " + result.getGuid());

		FilePath fromIndexPath = new LocalFilePath(result.getIndexPath());
		FilePath toIndexPath = baseDir.newFilePath(result.getGuid() + ".idx");

		FilePath fromDataPath = new LocalFilePath(result.getDataPath());
		FilePath toDataPath = baseDir.newFilePath(result.getGuid() + ".dat");

		try {
			copy(fromIndexPath, toIndexPath);
			copy(fromDataPath, toDataPath);

			writeSaveResult();
		} catch (IOException e) {
			toIndexPath.delete();
			toDataPath.delete();
			throw e;
		}
	}

	private ConcurrentHashMap<String, SavedResult> readSavedResults() {
		File resultFile = new File(((LocalFilePath) baseDir).getFile(), FILE_NAME);
		if (!resultFile.exists()) {
			slog.debug("araqne logdb: saved result file [{}] does not exist", resultFile.getAbsolutePath());
			return null;
		}

		FileInputStream fis = null;
		BufferedReader br = null;
		try {
			ConcurrentHashMap<String, SavedResult> results = new ConcurrentHashMap<String, SavedResult>();
			fis = new FileInputStream(resultFile);
			br = new BufferedReader(new InputStreamReader(fis, "utf-8"));
			String line = null;
			while ((line = br.readLine()) != null) {
				Map<String, Object> m = JSONConverter.parse(new JSONObject(line));
				SavedResult result = SavedResult.parse(m);
				results.put(result.getGuid(), result);
			}

			return results;
		} catch (Throwable t) {
			slog.error("araqne logdb: cannot read saved result file [" + resultFile.getAbsolutePath() + "]", t);
			return new ConcurrentHashMap<String, SavedResult>();
		} finally {
			IoHelper.close(br);
			IoHelper.close(fis);
		}

	}

	private void writeSaveResult() {
		File resultFile = new File(((LocalFilePath) baseDir).getFile(), FILE_NAME);
		File tmpFile = new File(((LocalFilePath) baseDir).getFile(), FILE_NAME + ".tmp");
		FileOutputStream fos = null;
		BufferedWriter bw = null;
		try {
			fos = new FileOutputStream(tmpFile);
			bw = new BufferedWriter(new OutputStreamWriter(fos, "utf-8"));

			for (SavedResult result : cachedResults.values()) {
				String json = JSONConverter.jsonize(result.marshal());
				bw.write(json);
				bw.newLine();
			}

			bw.flush();
			fos.flush();
			fos.getFD().sync();
		} catch (Throwable t) {
			slog.warn("araqne logdb: cannot write saved results. file path [" + tmpFile.getAbsolutePath() + "]", t);
			return;
		} finally {
			IoHelper.close(bw);
			IoHelper.close(fos);
		}

		if (resultFile.exists()) {
			if (!resultFile.delete()) {
				slog.error("araqne logdb: cannot delete old saved results [{}]", resultFile.getAbsolutePath());
			}
		}

		tmpFile.renameTo(resultFile);
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
		writeSaveResult();
		cachedResults.remove(guid);

		FilePath indexPath = baseDir.newFilePath(guid + ".idx");
		FilePath dataPath = baseDir.newFilePath(guid + ".dat");

		if (!indexPath.delete())
			slog.error("araqne logdb: cannot delete saved result [{}]", indexPath.getAbsolutePath());
		if (!dataPath.delete())
			slog.error("araqne logdb: cannot delete saved result [{}]", dataPath.getAbsolutePath());
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
			this.c = reader.getCursor(true);
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
