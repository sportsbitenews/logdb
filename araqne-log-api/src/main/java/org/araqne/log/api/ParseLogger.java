package org.araqne.log.api;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ParseLogger extends AbstractLogger implements LoggerRegistryEventListener {
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(ParseLogger.class.getName());
	private LoggerRegistry loggerRegistry;
	private LogParserRegistry parserRegistry;
	/**
	 * full name of data source logger
	 */
	private String loggerName;
	private LogParser parser;

	private volatile boolean stopRunner = false;
	private ParseRunner runner;

	private Receiver receiver = new Receiver();
	private ArrayBlockingQueue<Log[]> queue = new ArrayBlockingQueue<Log[]>(100000);

	public ParseLogger(LoggerSpecification spec, LoggerFactory factory, LoggerRegistry loggerRegistry,
			LogParserRegistry parserRegistry) {
		super(spec, factory);
		this.loggerRegistry = loggerRegistry;
		this.parserRegistry = parserRegistry;
		Map<String, String> config = spec.getConfig();
		loggerName = config.get("source_logger");
	}

	@Override
	protected void onStart() {
		if (runner == null) {
			runner = new ParseRunner();
			runner.start();
		}

		parser = parserRegistry.newParser(getConfigs().get("parser_name"));
		loggerRegistry.addListener(this);
		Logger logger = loggerRegistry.getLogger(loggerName);

		if (logger != null) {
			slog.debug("araqne log api: connect pipe to source logger [{}]", loggerName);
			logger.addLogPipe(receiver);
		} else
			slog.debug("araqne log api: source logger [{}] not found", loggerName);
	}

	@Override
	protected void onStop(LoggerStopReason reason) {
		try {
			stopRunner = true;
			runner.interrupt();
			try {
				runner.join(5000);
			} catch (InterruptedException e) {
				slog.info("araqne log api: failed to join parse runner, logger [{}]", getFullName());
			}

			if (loggerRegistry != null) {
				Logger logger = loggerRegistry.getLogger(loggerName);
				if (logger != null) {
					slog.debug("araqne log api: disconnect pipe from source logger [{}]", loggerName);
					logger.removeLogPipe(receiver);
				}

				loggerRegistry.removeListener(this);
			}
		} catch (RuntimeException e) {
			if (e.getMessage() == null || !e.getMessage().endsWith("unavailable"))
				throw e;
		}
	}

	@Override
	public boolean isPassive() {
		return true;
	}

	@Override
	protected void runOnce() {
	}

	@Override
	public void loggerAdded(Logger logger) {
		if (logger.getFullName().equals(loggerName)) {
			slog.debug("araqne log api: source logger [{}] loaded", loggerName);
			logger.addLogPipe(receiver);
		}
	}

	@Override
	public void loggerRemoved(Logger logger) {
		if (logger.getFullName().equals(loggerName)) {
			slog.debug("araqne log api: source logger [{}] unloaded", loggerName);
			logger.removeLogPipe(receiver);
		}
	}

	private class Receiver extends AbstractLogPipe {

		@Override
		public void onLog(Logger logger, Log log) {
			try {
				if (isRunning())
					queue.put(new Log[] { log });
			} catch (Throwable t) {
				slog.error("araqne log api: cannot parse log [" + log.getParams() + "]", t);
			}
		}

		@Override
		public void onLogBatch(Logger logger, Log[] logs) {
			try {
				if (isRunning())
					queue.put(logs);
			} catch (Throwable t) {
				slog.error("araqne log api: cannot parse log [" + logs + "]", t);
			}
		}
	}

	private class ParseRunner extends Thread {
		@Override
		public void run() {
			try {
				Log log = null;
				slog.info("araqne log api: begin parser runner, logger [{}]", getFullName());
				while (!stopRunner) {
					Log[] logs = null;
					try {
						logs = queue.poll(1, TimeUnit.SECONDS);
						if (logs == null)
							continue;

						Log[] copy = Arrays.copyOf(logs, logs.length);

						for (int i = 0; i < logs.length; i++) {
							log = logs[i];
							if (log == null)
								continue;

							Map<String, Object> row = parser.parse(log.getParams());
							if (row == null)
								continue;

							copy[i] = new SimpleLog(log.getDate(), getFullName(), row);
						}

						writeBatch(copy);
					} catch (Throwable t) {
						if (logs != null && log != null)
							slog.error("araqne log api: cannot parse log [" + log.getParams() + "]", t);
					}
				}
			} catch (Throwable t) {
				slog.error("araqne log api: parser runner failed", t);
			} finally {
				stopRunner = false;
				runner = null;
			}
		}
	}
}
