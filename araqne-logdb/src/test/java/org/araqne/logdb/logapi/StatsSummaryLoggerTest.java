package org.araqne.logdb.logapi;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.araqne.log.api.*;
import org.araqne.logdb.logapi.StatsSummaryLogger;
import org.araqne.logdb.logapi.StatsSummaryLoggerFactory;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class StatsSummaryLoggerTest {
	private static org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(StatsSummaryLoggerTest.class);

	public static class SampleLogger extends AbstractLogger {
		private int runCount;

		public SampleLogger(LoggerSpecification spec, LoggerFactory factory, int runCount) {
			super(spec, factory);
			this.runCount = runCount;
		}

		@SuppressWarnings("serial")
		public static Map<String, Object> l(final String type, final String line) {
			return new HashMap<String, Object>() {
				{
					put("type", type);
					put("line", line);
				}
			};
		}

		public List<String> readAllLines(File file, Charset cs) throws IOException {
			BufferedReader reader = null;
			try {
				ArrayList<String> lines = new ArrayList<String>();
				FileInputStream fis = new FileInputStream(file);
				reader = new BufferedReader(new InputStreamReader(fis, cs));
				String line = null;
				while ((line = reader.readLine()) != null) {
					lines.add(line);
				}
				return lines;
			} finally {
				if (reader != null)
					try {
						reader.close();
					} catch (IOException e) {

					}
			}
		}

		@Override
		protected void runOnce() {
			slog.trace("source - runOnce called");

			try {
				List<String> lines = readAllLines(new File(this.getClass().getResource("/SummaryLoggerTest-Sample1.txt").toURI()),
						Charset.forName("utf-8"));
				for (String line : lines) {
					Map<String, Object> data = parseLine(line);
					write(new SimpleLog((Date) data.get("date"), this.getName(), data));
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
			if (--runCount == 0)
				synchronized (this) {
					this.notifyAll();
				}
		}

		private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		private Map<String, Object> parseLine(String line) {
			HashMap<String, Object> m = new HashMap<String, Object>();
			ParsePosition pp = new ParsePosition(0);
			m.put("date", sdf.parse(line, pp));
			line = line.substring(pp.getIndex() + 1);
			String[] terms = line.split("[ ]");
			m.put("rid", Integer.parseInt(terms[0]));
			m.put("cat1", terms[1]);
			m.put("cat2", terms[2]);
			m.put("val1", Integer.parseInt(terms[3]));
			m.put("val2", Integer.parseInt(terms[4]));

			return m;
		}
	}

	public static class SampleLoggerFactory extends AbstractLoggerFactory {
		@Override
		public String getName() {
			return "sample-logger-factory";
		}

		@Override
		public String getDisplayName(Locale locale) {
			return "sample-logger-factory";
		}

		@Override
		public String getDescription(Locale locale) {
			return "";
		}

		@Override
		protected Logger createLogger(LoggerSpecification spec) {
			return new SampleLogger(spec, this, 1);
		}

	}

	@Test
	public void test() {
		LoggerFactory factory = mock(LoggerFactory.class);
		LoggerRegistry loggerRegistry = mock(LoggerRegistry.class);
		LogParserRegistry parserRegistry = mock(LogParserRegistry.class);

		LogPipe sink = mock(LogPipe.class);

		final ArrayList<Log> logs = new ArrayList<Log>();

		doAnswer(new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				Object[] arguments = invocation.getArguments();
				logs.add((Log) arguments[1]);
				return null;
			}
		}).when(sink).onLog(any(Logger.class), any(Log.class));

		SampleLogger sourceLogger = new SampleLogger(
				new LoggerSpecification("local", "source", "", 0, null, 0, new HashMap<String, String>()),
				new SampleLoggerFactory(), 1);

		when(loggerRegistry.getLogger("local\\source")).thenReturn(sourceLogger);

		Map<String, String> config = new HashMap<String, String>();
		config.put(StatsSummaryLoggerFactory.OPT_SOURCE_LOGGER, "local\\source");
		config.put(StatsSummaryLoggerFactory.OPT_PARSER, "");
		config.put(StatsSummaryLoggerFactory.OPT_QUERY,
				"stats count, sum(val1) as sum_val1, avg(val1) as avg_val1, sum(val2) as sum_val2, avg(val2) as avg_val2 by cat1, cat2");
		config.put(StatsSummaryLoggerFactory.OPT_MIN_INTERVAL, "600");
		config.put(StatsSummaryLoggerFactory.OPT_FLUSH_INTERVAL, "1");
		config.put(StatsSummaryLoggerFactory.OPT_MEMORY_ITEMSIZE, "50000");
		LoggerSpecification spec = new LoggerSpecification("local", "test", "", 0, null, 0, config);

		StatsSummaryLogger logger = new StatsSummaryLogger(spec, factory, loggerRegistry, parserRegistry);

		try {
			logger.addLogPipe(sink);
			logger.start(1000);

			startAndWaitForRunOnce(sourceLogger);

			slog.trace("source logger stopped");

			logger.setForceFlush();
			logger.runOnce(); // flush
			logger.stop(5000);

			// check the result of sink
			Map<String, long[]> rm = new HashMap<String, long[]>();

			for (Log l : logs) {
				String c1 = (String) l.getParams().get("cat1");
				String c2 = (String) l.getParams().get("cat2");
				long v1 = (Long) l.getParams().get("sum_val1");
				long v2 = (Long) l.getParams().get("sum_val2");
				long c = (Long) l.getParams().get("count");

				long[] v = rm.get(c1 + c2);
				if (v == null) {
					v = new long[3];
					rm.put(c1 + c2, v);
				}
				v[0] += c;
				v[1] += v1;
				v[2] += v2;
			}

			assertArrayEquals(rm.get("AX"), new long[] { 691, 6590, 75694 });
			assertArrayEquals(rm.get("BX"), new long[] { 691, 13546, 82538 });
			assertArrayEquals(rm.get("CX"), new long[] { 1003, 29633, 130000 });
			assertArrayEquals(rm.get("AY"), new long[] { 338, 3141, 36973 });
			assertArrayEquals(rm.get("BY"), new long[] { 352, 6938, 42074 });
			assertArrayEquals(rm.get("CY"), new long[] { 525, 15590, 67903 });

		} finally {
			if (logger.isRunning())
				logger.stop();
			if (sourceLogger.isRunning())
				sourceLogger.stop();
		}
	}

	private void startAndWaitForRunOnce(SampleLogger sourceLogger) {
		try {
			synchronized (sourceLogger) {
				sourceLogger.start(1000);
				sourceLogger.wait();
				sourceLogger.stop();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}

