package org.araqne.logstorage.file;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.araqne.logstorage.Log;
import org.araqne.logstorage.LogFlushCallback;
import org.araqne.logstorage.LogFlushCallbackArgs;
import org.araqne.logstorage.LogMarshaler;
import org.araqne.logstorage.LogMatchCallback;
import org.araqne.logstorage.LogTraverseCallback;
import org.araqne.logstorage.LogTraverseCallback.Sink;
import org.araqne.logstorage.SimpleLogTraverseCallback;
import org.araqne.storage.api.FilePath;
import org.araqne.storage.localfile.LocalFilePath;
import org.junit.Test;

public class LogFlushCallbackTest {
	@Test
	public void testv2() throws InvalidLogFileHeaderException, IOException {
		final AtomicInteger fcnt = new AtomicInteger(0);
		final AtomicInteger cmplcnt = new AtomicInteger(0);
		Set<LogFlushCallback> flushCallbacks = new HashSet<LogFlushCallback>();
		flushCallbacks.add(new LogFlushCallback() {
			@Override
			public void onFlushException(LogFlushCallbackArgs arg, Throwable t) {

			}

			@Override
			public void onFlushCompleted(LogFlushCallbackArgs args) {
				cmplcnt.incrementAndGet();
			}

			@Override
			public void onFlush(LogFlushCallbackArgs arg) {
				fcnt.incrementAndGet();
			}
		});
		FilePath indexPath = new LocalFilePath("lfctestv2.idx");
		FilePath dataPath = new LocalFilePath("lfctestv2.dat");

		indexPath.deleteOnExit();
		dataPath.deleteOnExit();

		LogFileWriterV2 writer = new LogFileWriterV2(indexPath, dataPath, flushCallbacks, new LogFlushCallbackArgs("test"));

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 100; ++i) {
			sb.append("data");
		}
		String line = sb.toString();

		int testcnt = 15100;

		try {
			for (int i = 0; i < testcnt; ++i) {
				Map<String, Object> logdata = new HashMap<String, Object>();
				logdata.put("line", line);
				writer.write(new Log("test", new Date(), i + 1, logdata));
			}
		} finally {
			writer.close();
		}

		System.out.println("flush count: " + fcnt.get());
		System.out.println("completed flush count: " + cmplcnt.get());
		assertTrue("fcnt.get() == 0", 0 != fcnt.get());
		assertEquals(fcnt.get(), cmplcnt.get());

		LogFileReaderV2 reader = null;
		try {
			reader = new LogFileReaderV2("test", indexPath, dataPath);
			LogRecordCursor cursor = reader.getCursor();

			int logcnt = 0;
			while (cursor.hasNext()) {
				LogRecord next = cursor.next();
				Log converted = LogMarshaler.convert("test", next);
				logcnt++;
			}

			assertEquals(testcnt, logcnt);

			reader.close();
		} finally {
			if (reader != null)
				reader.close();
		}
	}

	@Test
	public void testv1() throws InvalidLogFileHeaderException, IOException, InterruptedException {
		final AtomicInteger fcnt = new AtomicInteger(0);
		final AtomicInteger cmplcnt = new AtomicInteger(0);
		Set<LogFlushCallback> flushCallbacks = new HashSet<LogFlushCallback>();
		flushCallbacks.add(new LogFlushCallback() {
			@Override
			public void onFlushException(LogFlushCallbackArgs arg, Throwable t) {

			}

			@Override
			public void onFlushCompleted(LogFlushCallbackArgs args) {
				cmplcnt.incrementAndGet();
			}

			@Override
			public void onFlush(LogFlushCallbackArgs arg) {
				fcnt.incrementAndGet();
			}
		});
		File indexFile = new File("lfctestv1.idx");
		File dataFile = new File("lfctestv1.dat");

		indexFile.deleteOnExit();
		dataFile.deleteOnExit();

		LogFileWriterV1 writer = new LogFileWriterV1(indexFile, dataFile, flushCallbacks, new LogFlushCallbackArgs("test"));

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 100; ++i) {
			sb.append("data");
		}
		String line = sb.toString();

		int testcnt = 15100;

		try {
			for (int i = 0; i < testcnt; ++i) {
				Map<String, Object> logdata = new HashMap<String, Object>();
				logdata.put("line", line);
				writer.write(new Log("test", new Date(), i + 1, logdata));
			}
		} finally {
			writer.close();
		}

		System.out.println("flush count: " + fcnt.get());
		System.out.println("completed flush count: " + cmplcnt.get());
		assertTrue("fcnt.get() == 0", 0 != fcnt.get());
		assertEquals(fcnt.get(), cmplcnt.get());

		LogFileReaderV1 reader = null;
		try {
			FilePath indexPath = new LocalFilePath("lfctestv1.idx");
			FilePath dataPath = new LocalFilePath("lfctestv1.dat");

			reader = new LogFileReaderV1("test", indexPath, dataPath);

			final AtomicInteger logcnt = new AtomicInteger();

			reader.traverse(null, null, -1, -1, null, new SimpleLogTraverseCallback(new Sink(0, 0) {
				@Override
				protected void processLogs(List<Log> logs) {
					logcnt.addAndGet(logs.size());
				}
			}), false);
			assertEquals(testcnt, logcnt.get());

			reader.close();
		} finally {
			if (reader != null)
				reader.close();
		}
	}

}
