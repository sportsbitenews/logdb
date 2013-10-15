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
package org.araqne.logdb.summary;

import java.util.Map;

import org.araqne.log.api.AbstractLogger;
import org.araqne.log.api.Log;
import org.araqne.log.api.LogPipe;
import org.araqne.log.api.Logger;
import org.araqne.log.api.LoggerFactory;
import org.araqne.log.api.LoggerRegistry;
import org.araqne.log.api.LoggerRegistryEventListener;
import org.araqne.log.api.LoggerSpecification;
import org.araqne.log.api.SimpleLog;

public class SummaryLogger extends AbstractLogger implements LoggerRegistryEventListener, LogPipe {
	private static final String OPT_SOURCE_LOGGER = "source_logger";
	private static final String OPT_QUERY = "stats_query";
	private static final String OPT_MIN_INTERVAL = "aggr_interval"; 
	private static final String OPT_FLUSH_INTERVAL = "flush_interval";
	private static final String OPT_MEMORY_ITEMSIZE = "max_itemsize";

	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(SummaryLogger.class.getName());
	private LoggerRegistry loggerRegistry;

	/**
	 * full name of data source logger
	 */
	private String loggerName;

	private String pattern;
	private String queryString;
	private int aggrInterval;
	private int flushInterval;
	private int maxItemSize;

	public SummaryLogger(LoggerSpecification spec, LoggerFactory factory, LoggerRegistry loggerRegistry) {
		super(spec, factory);
		this.loggerRegistry = loggerRegistry;
		Map<String, String> config = spec.getConfig();
		loggerName = config.get(OPT_SOURCE_LOGGER);
		queryString = config.get(OPT_QUERY);
		aggrInterval = Integer.parseInt(config.get(OPT_MIN_INTERVAL));
		flushInterval = Integer.parseInt(config.get(OPT_FLUSH_INTERVAL));
		maxItemSize = Integer.parseInt(config.get(OPT_MEMORY_ITEMSIZE));
	}

	@Override
	protected void onStart() {
		loggerRegistry.addListener(this);
		Logger logger = loggerRegistry.getLogger(loggerName);

		if (logger != null) {
			slog.debug("araqne log api: connect pipe to source logger [{}]", loggerName);
			logger.addLogPipe(this);
		} else
			slog.debug("araqne log api: source logger [{}] not found", loggerName);
	}

	@Override
	protected void onStop() {
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

		if (line.startsWith(pattern)) {
			write(new SimpleLog(log.getDate(), getFullName(), log.getParams()));
		}
	}
}
