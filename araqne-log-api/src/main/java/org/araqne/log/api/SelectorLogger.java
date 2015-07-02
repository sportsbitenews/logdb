/*
 * Copyright 2013 Eediom Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.araqne.log.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class SelectorLogger extends AbstractLogger implements LoggerRegistryEventListener, Reconfigurable {
	private static final String OPT_SOURCE_LOGGER = "source_logger";
	private static final String OPT_PATTERN = "pattern";
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(SelectorLogger.class.getName());
	private LoggerRegistry loggerRegistry;

	/**
	 * full name of data source logger
	 */
	private String loggerName;

	private String pattern;

	private Receiver receiver = new Receiver();
	private Logger sourceLogger;

	public SelectorLogger(LoggerSpecification spec, LoggerFactory factory, LoggerRegistry loggerRegistry) {
		super(spec, factory);
		this.loggerRegistry = loggerRegistry;
		this.loggerRegistry.addListener(this);

		Map<String, String> config = spec.getConfig();
		this.loggerName = config.get(OPT_SOURCE_LOGGER);
		this.pattern = config.get(OPT_PATTERN);
		this.sourceLogger = loggerRegistry.getLogger(loggerName);
	}

	@Override
	public void onConfigChange(Map<String, String> oldConfigs, Map<String, String> newConfigs) {
		this.loggerName = newConfigs.get(OPT_SOURCE_LOGGER);
		this.pattern = newConfigs.get(OPT_PATTERN);
	}

	@Override
	protected void onStart(LoggerStartReason reason) {
		Logger captured = sourceLogger;
		if (captured != null) {
			slog.debug("araqne log api: connect pipe to source logger [{}]", loggerName);
			captured.addLogPipe(receiver);
		} else
			slog.debug("araqne log api: source logger [{}] not found", loggerName);
	}

	@Override
	protected void onStop(LoggerStopReason reason) {
		try {
			Logger captured = sourceLogger;
			if (captured != null) {
				slog.debug("araqne log api: disconnect pipe from source logger [{}]", loggerName);
				captured.removeLogPipe(receiver);
				loggerRegistry.removeListener(this);
			}
		} catch (RuntimeException e) {
			if (!e.getMessage().endsWith("unavailable"))
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
			if (isRunning()) {
				slog.debug("araqne log api: connect pipe to source logger [{}]", loggerName);
				logger.addLogPipe(receiver);
			}

			this.sourceLogger = logger;
		}
	}

	@Override
	public void loggerRemoved(Logger logger) {
		if (logger.getFullName().equals(loggerName)) {
			slog.debug("araqne log api: source logger [{}] unloaded", loggerName);
			slog.debug("araqne log api: disconnect pipe from source logger [{}]", loggerName);
			logger.removeLogPipe(receiver);
			this.sourceLogger = null;
		}
	}

	private class Receiver extends AbstractLogPipe {
		@Override
		public void onLog(Logger logger, Log log) {
			String line = (String) log.getParams().get("line");
			if (line == null)
				return;

			if (line.startsWith(pattern)) {
				write(new SimpleLog(log.getDate(), getFullName(), log.getParams()));
			}
		}

		@Override
		public void onLogBatch(Logger logger, Log[] logs) {
			ArrayList<Log> selected = new ArrayList<Log>(logs.length);

			for (int i = 0; i < logs.length; i++) {
				Log log = logs[i];
				if (log == null)
					continue;

				Map<String, Object> params = log.getParams();
				String line = (String) params.get("line");
				if (line != null && line.startsWith(pattern)) {
					params = new HashMap<String, Object>(params);
					selected.add(new SimpleLog(log.getDate(), getFullName(), params));
				}
			}

			writeBatch(selected.toArray(new Log[0]));
		}
	}
}
