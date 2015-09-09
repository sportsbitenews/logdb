/*
 * Copyright 2014 Eediom Inc.
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

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexSelectorLogger extends AbstractLogger implements LoggerRegistryEventListener, LogPipe, Reconfigurable {
	private static final String OPT_SOURCE_LOGGER = "source_logger";
	private static final String OPT_PATTERN = "pattern";
	private static final String OPT_INVERT = "invert";
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(RegexSelectorLogger.class.getName());
	private LoggerRegistry loggerRegistry;

	/**
	 * full name of data source logger
	 */
	private String loggerName;

	private String pattern;

	private boolean invert;

	/**
	 * cached pattern matchers per thread
	 */
	private ThreadLocal<Matcher> matchers = new ThreadLocal<Matcher>();

	public RegexSelectorLogger(LoggerSpecification spec, LoggerFactory factory, LoggerRegistry loggerRegistry) {
		super(spec, factory);
		this.loggerRegistry = loggerRegistry;
		Map<String, String> config = spec.getConfig();
		loggerName = config.get(OPT_SOURCE_LOGGER);
		pattern = config.get(OPT_PATTERN);
		invert = config.get(OPT_INVERT) != null && Boolean.parseBoolean(config.get(OPT_INVERT));
	}

	@Override
	public void onConfigChange(Map<String, String> oldConfigs, Map<String, String> newConfigs) {
		this.loggerName = newConfigs.get(OPT_SOURCE_LOGGER);
		this.pattern = newConfigs.get(OPT_PATTERN);
		if (!oldConfigs.get(OPT_PATTERN).equals(newConfigs.get(OPT_PATTERN)))
			matchers.remove();
		this.invert = newConfigs.get(OPT_INVERT) != null && Boolean.parseBoolean(newConfigs.get(OPT_INVERT));
	}

	@Override
	protected void onStart(LoggerStartReason reason) {
		loggerRegistry.addListener(this);
		Logger logger = loggerRegistry.getLogger(loggerName);

		if (logger != null) {
			slog.debug("araqne log api: connect pipe to source logger [{}]", loggerName);
			logger.addLogPipe(this);
		} else
			slog.debug("araqne log api: source logger [{}] not found", loggerName);
	}

	@Override
	protected void onStop(LoggerStopReason reason) {
		try {
			if (loggerRegistry != null) {
				Logger logger = loggerRegistry.getLogger(loggerName);
				if (logger != null) {
					slog.debug("araqne log api: disconnect pipe from source logger [{}]", loggerName);
					logger.removeLogPipe(this);
				}

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
			logger.addLogPipe(this);
		}
	}

	@Override
	public void loggerRemoved(Logger logger) {
		if (logger.getFullName().equals(loggerName)) {
			slog.debug("araqne log api: source logger [{}] unloaded", loggerName);
			logger.removeLogPipe(this);
		}
	}

	@Override
	public void onLog(Logger logger, Log log) {
		String line = (String) log.getParams().get("line");
		if (line == null)
			return;

		Matcher matcher = matchers.get();
		if (matcher == null) {
			matcher = Pattern.compile(pattern).matcher(line);
			matchers.set(matcher);
		} else {
			matcher.reset(line);
		}

		boolean select = matcher.find();
		if (invert)
			select = !select;

		if (select)
			write(new SimpleLog(log.getDate(), getFullName(), log.getParams()));
	}

	@Override
	public void onLogBatch(Logger logger, Log[] logs) {
		for (Log log : logs)
			if (log != null)
				onLog(logger, log);
	}
}