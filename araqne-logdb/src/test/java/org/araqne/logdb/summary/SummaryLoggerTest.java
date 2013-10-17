/*package org.araqne.logdb.summary;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.araqne.log.api.AbstractLogger;
import org.araqne.log.api.AbstractLoggerFactory;
import org.araqne.log.api.Log;
import org.araqne.log.api.LogPipe;
import org.araqne.log.api.Logger;
import org.araqne.log.api.LoggerFactory;
import org.araqne.log.api.LoggerRegistry;
import org.araqne.log.api.LoggerSpecification;
import org.araqne.log.api.SimpleLog;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class SummaryLoggerTest {
	private static org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(SummaryLoggerTest.class);

	public static class SampleLogger extends AbstractLogger {
		public SampleLogger(LoggerSpecification spec, LoggerFactory factory) {
			super(spec, factory);
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

		private static Path toPath(URL url) {
			try {
				return new File(url.toURI()).toPath();
			} catch (URISyntaxException e) {
				return null;
			}
		}

		@Override
		protected void runOnce() {
			slog.trace("source - runOnce called");

			try {
				List<String> lines = Files.readAllLines(toPath(this.getClass().getResource("/SummaryLoggerTest-Sample1.txt")),
						Charset.forName("utf-8"));
				for (String line : lines) {
					Map<String, Object> data = parseLine(line);
					write(new SimpleLog((Date) data.get("date"), this.getName(), data));
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			synchronized (this) {
				this.notify();
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
			return new SampleLogger(spec, this);
		}

	}

	@Test
	public void test() {
		LoggerFactory factory = mock(LoggerFactory.class);
		LoggerRegistry loggerRegistry = mock(LoggerRegistry.class);

		LogPipe sink = mock(LogPipe.class);

		doAnswer(new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				Object[] arguments = invocation.getArguments();
				System.out.println(arguments[1].toString());
				return null;
			}
		}).when(sink).onLog(any(Logger.class), any(Log.class));

		SampleLogger sourceLogger = new SampleLogger(
				new LoggerSpecification("local", "source", "", 0, null, 0, new HashMap<String, String>()),
				new SampleLoggerFactory());

		when(loggerRegistry.getLogger("local\\source")).thenReturn(sourceLogger);

		Map<String, String> config = new HashMap<String, String>();
		config.put(SummaryLoggerFactory.OPT_SOURCE_LOGGER, "local\\source");
		config.put(SummaryLoggerFactory.OPT_QUERY, "stats count, sum(val1), avg(val1), sum(val2), avg(val2) by cat1, cat2");
		config.put(SummaryLoggerFactory.OPT_MIN_INTERVAL, "60");
		config.put(SummaryLoggerFactory.OPT_FLUSH_INTERVAL, "60");
		config.put(SummaryLoggerFactory.OPT_MEMORY_ITEMSIZE, "60");
		LoggerSpecification spec = new LoggerSpecification("local", "test", "", 0, null, 0, config);

		SummaryLogger logger = new SummaryLogger(spec, factory, loggerRegistry);

		try {
			logger.addLogPipe(sink);
			logger.start(1000);

			startAndWaitForRunOnce(sourceLogger);

			logger.runOnce(); // flush

			// check the result of sink

			logger.stop(5000);
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
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
*/