package org.araqne.logstorage.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.araqne.logstorage.CallbackSet;
import org.araqne.logstorage.Log;
import org.araqne.logstorage.LogFlushCallback;
import org.araqne.logstorage.LogFlushCallbackArgs;
import org.araqne.logstorage.LogMarshaler;
import org.araqne.logstorage.LogUtil;
import org.araqne.storage.api.FilePath;
import org.araqne.storage.localfile.LocalFilePath;
import org.junit.Test;

public class LogFlushCallbackTest {
	@Test
	public void testv2() throws InvalidLogFileHeaderException, IOException {
		final AtomicInteger fcnt = new AtomicInteger(0);
		final AtomicInteger cmplcnt = new AtomicInteger(0);
		CallbackSet cbSet = new CallbackSet();
		cbSet.get(LogFlushCallback.class).add(new LogFlushCallback() {
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

		LogFileWriterV2 writer = new LogFileWriterV2(indexPath, dataPath, cbSet, "test", LogUtil.getDay(new Date()));

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
}
