package org.araqne.logstorage.file;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.araqne.logstorage.CallbackSet;
import org.araqne.logstorage.Log;
import org.araqne.logstorage.LogFlushCallback;
import org.araqne.logstorage.LogFlushCallbackArgs;
import org.araqne.logstorage.LogMarshaler;
import org.araqne.logstorage.file.InvalidLogFileHeaderException;
import org.araqne.logstorage.file.LogRecord;
import org.araqne.logstorage.file.LogRecordCursor;
import org.araqne.storage.api.FilePath;
import org.araqne.storage.localfile.LocalFilePath;
import org.junit.Test;

import com.google.common.base.Stopwatch;

public class LogFileWriterV3oTest {
	@Test
	public void hangTest() throws InvalidLogFileHeaderException, IOException {
		LogWriterConfigV3o config = new LogWriterConfigV3o();
		config.setTableName("lfwv3test");
		LocalFilePath indexPath = new LocalFilePath("lfwv3test.idx");
		config.setIndexPath(indexPath);
		LocalFilePath dataPath = new LocalFilePath("lfwv3test.dat");
		config.setDataPath(dataPath);
		config.setListener(null);
		config.setFlushCount(200);
		config.setCompression("deflate");
		config.setCallbackSet(null);

		indexPath.deleteOnExit();
		dataPath.deleteOnExit();

		Random rand1 = new Random(1);
		Random rand2 = new Random(2);

		ArrayList<String> s1 = new ArrayList<String>(15100);
		ArrayList<String> s2 = new ArrayList<String>(15100);

		for (int i = 0; i < 15100; ++i) {
			s1.add("token" + rand1.nextInt(10000));
			s2.add("token" + rand2.nextInt(10000));
		}

		LogFileWriterV3o writer = null;
		try {
			writer = new LogFileWriterV3o(config);

			Stopwatch w = new Stopwatch();
			w.start();
			for (int i = 0; i < 15100; ++i) {
				Map<String, Object> logdata = new HashMap<String, Object>();
				logdata.put("line", String.format("%s,%s", s1.get(i), s2.get(i)));
				writer.write(new Log("lfwv3test", new Date(), i + 1, logdata));
			}
			System.out.println("input elapsed: " + w.elapsed(TimeUnit.MILLISECONDS));
		} finally {
			if (writer != null)
				writer.close();
		}

		LogReaderConfigV3o readerConfig = new LogReaderConfigV3o();
		readerConfig.checkIntegrity = false;
		readerConfig.dataPath = dataPath;
		readerConfig.indexPath = indexPath;
		readerConfig.tableName = "lfwv3test";

		LogFileReaderV3o reader = null;
		try {
			reader = new LogFileReaderV3o(readerConfig);
			LogRecordCursor cursor = reader.getCursor();
			for (int i = 15100; i > 0; --i) {
				LogRecord next = cursor.next();
				Log converted = LogMarshaler.convert("lfwv3test", next);
				assertEquals(String.format("%s,%s", s1.get(i - 1), s2.get(i - 1)), converted.getData().get("line"));
			}
		} finally {
			if (reader != null)
				reader.close();
		}
	}

	@Test
	public void flushCallbacktest() throws InvalidLogFileHeaderException, IOException {
		LogWriterConfigV3o config = new LogWriterConfigV3o();
		config.setTableName("lfwv3test");
		FilePath indexPath = new LocalFilePath("lfwv3test-lfc.idx");
		config.setIndexPath(indexPath);
		FilePath dataPath = new LocalFilePath("lfwv3test-lfc.dat");
		config.setDataPath(dataPath);
		config.setListener(null);
		config.setFlushCount(200);
		config.setCompression("deflate");

		final AtomicInteger fcnt = new AtomicInteger(0);
		final AtomicInteger cmplcnt = new AtomicInteger(0);

		CallbackSet callbackSet = new CallbackSet();
		callbackSet.get(LogFlushCallback.class).add(new LogFlushCallback() {
			@Override
			public void onFlushCompleted(LogFlushCallbackArgs args) {
				cmplcnt.incrementAndGet();
			}

			@Override
			public void onFlush(LogFlushCallbackArgs arg) {
				fcnt.incrementAndGet();
			}

			@Override
			public void onFlushException(LogFlushCallbackArgs arg, Throwable t) {
				System.err.println("exception on test");
				t.printStackTrace();
			}
		});

		config.setCallbackSet(callbackSet);

		indexPath.deleteOnExit();
		dataPath.deleteOnExit();

		Random rand1 = new Random(1);
		Random rand2 = new Random(2);

		ArrayList<String> s1 = new ArrayList<String>(15100);
		ArrayList<String> s2 = new ArrayList<String>(15100);

		for (int i = 0; i < 15100; ++i) {
			s1.add("token" + rand1.nextInt(10000));
			s2.add("token" + rand2.nextInt(10000));
		}

		LogFileWriterV3o writer = null;
		try {
			writer = new LogFileWriterV3o(config);

			Stopwatch w = new Stopwatch();
			w.start();
			for (int i = 0; i < 15100; ++i) {
				Map<String, Object> logdata = new HashMap<String, Object>();
				logdata.put("line", String.format("%s,%s", s1.get(i), s2.get(i)));
				writer.write(new Log("lfwv3test", new Date(), i + 1, logdata));
			}
			System.out.println("lfctest: input elapsed: " + w.elapsed(TimeUnit.MILLISECONDS));
		} finally {
			if (writer != null)
				writer.close();
		}

		LogReaderConfigV3o readerConfig = new LogReaderConfigV3o();
		readerConfig.checkIntegrity = false;
		readerConfig.dataPath = dataPath;
		readerConfig.indexPath = indexPath;
		readerConfig.tableName = "lfwv3test";

		LogFileReaderV3o reader = null;
		try {
			reader = new LogFileReaderV3o(readerConfig);
			LogRecordCursor cursor = reader.getCursor();
			for (int i = 15100; i > 0; --i) {
				LogRecord next = cursor.next();
				Log converted = LogMarshaler.convert("lfwv3test", next);
				assertEquals(String.format("%s,%s", s1.get(i - 1), s2.get(i - 1)), converted.getData().get("line"));
			}
		} finally {
			if (reader != null)
				reader.close();
		}

		assertEquals(76, fcnt.get());
		assertEquals(76, cmplcnt.get());

	}
}
