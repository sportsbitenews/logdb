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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
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
import org.araqne.logdb.LogMap;
import org.araqne.logdb.LogQueryContext;
import org.araqne.logdb.Session;
import org.araqne.logdb.query.aggregator.AggregationField;
import org.araqne.logdb.query.aggregator.AggregationFunction;
import org.araqne.logdb.query.parser.AggregationParser;
import org.araqne.logdb.query.parser.StatsParser;
import org.araqne.logdb.query.parser.StatsParser.SyntaxParseResult;

public class SummaryLogger extends AbstractLogger implements LoggerRegistryEventListener, LogPipe {
	public class EmptySession implements Session {
		@Override
		public String getGuid() {
			return "";
		}

		@Override
		public String getLoginName() {
			return "user";
		}

		@Override
		public Date getCreated() {
			return new Date();
		}

		@Override
		public boolean isAdmin() {
			return false;
		}

	}

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
	private KeyExtractor keyExtractor;
	private AggregationFunction[] funcs;
	private List<String> clauses;
	private Date lastFlush = new Date();

	public SummaryLogger(LoggerSpecification spec, LoggerFactory factory, LoggerRegistry loggerRegistry) {
		super(spec, factory);
		this.loggerRegistry = loggerRegistry;
		Map<String, String> config = spec.getConfig();
		loggerName = config.get(OPT_SOURCE_LOGGER);
		queryString = config.get(OPT_QUERY);
		aggrInterval = Integer.parseInt(config.get(OPT_MIN_INTERVAL));
		flushInterval = Integer.parseInt(config.get(OPT_FLUSH_INTERVAL));
		maxItemSize = Integer.parseInt(config.get(OPT_MEMORY_ITEMSIZE));

		init();
	}

	static Map<String, Class<? extends AggregationFunction>> funcTable = new HashMap<String, Class<? extends AggregationFunction>>();
	{
		// funcTable.put("sum", org.araqne.logdb.summary.Sum.class);
	}

	private void init() {
		// sanitize queryString
		queryString = queryString.trim();
		if (!queryString.startsWith("stats "))
			queryString = "stats " + queryString;

		// get AST
		SyntaxParseResult pr = new StatsParser().parseSyntax(null, queryString);

		// generate aggr fields
		LogQueryContext context = new LogQueryContext(new EmptySession());
		clauses = pr.clauses;
		fields = new ArrayList<AggregationField>();
		for (String aggTerm : pr.aggTerms) {
			AggregationField field = AggregationParser.parse(context, aggTerm/* , funcTable */);
			fields.add(field);
		}

		keyExtractor = new KeyExtractor(aggrInterval, pr.clauses);

		funcs = new AggregationFunction[fields.size()];
		for (int i = 0; i < fields.size(); ++i) {
			this.funcs[i] = fields.get(i).getFunction();
		}

		for (AggregationFunction f : funcs) {
			f.clean();
		}
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
		return false;
	}

	@Override
	protected void runOnce() {
		// if needed, flush in memory items
		if (needFlush()) {
			flush();
		}
	}

	private boolean needFlush() {
		// XXX
		if (forceFlush) {
			forceFlush = false;
			return true;
		}
		if (buffer.size() >= maxItemSize)
			return true;
		if (lastFlush.getTime() / flushInterval * (flushInterval + 1) < new Date().getTime())
			return true;
		return false;
	}

	public void flush() {
		slog.trace("flush called");
		
		for (SummaryKey key: buffer.keySet()) {
			HashMap<String, Object> m = new HashMap<String, Object>();

			// put summary values
			AggregationFunction[] fs = buffer.get(key);
			for (int i = 0; i < fs.length; ++i) {
				m.put(fields.get(i).getName(), fs[i].eval());
			}
			
			// put key
			List<String> cs = keyExtractor.getClauses();
			for (int i = 0; i < key.size(); ++i) {
				m.put(cs.get(i), key.get(i));
			}
			
			write(new SimpleLog(key.getDate(), this.getName(), m));
		}
		
		lastFlush = new Date();
		
		buffer.clear();
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

	List<Log> logs = new ArrayList<Log>();
	private List<AggregationField> fields;
	private int inputCount;

	private Map<SummaryKey, AggregationFunction[]> buffer = new HashMap<SummaryKey, AggregationFunction[]>();
	private boolean forceFlush = false;

	@Override
	public void onLog(Logger logger, Log log) {
		String line = (String) log.getParams().get("line");
		SummaryKey key = keyExtractor.extract(log);
		if (slog.isDebugEnabled())
			slog.debug("{}", key);

		try {
			inputCount ++;
			AggregationFunction[] fs = buffer.get(key);
			if (fs == null) {
				fs = new AggregationFunction[funcs.length];
				for (int i = 0; i < fs.length; ++i) {
					fs[i] = funcs[i].clone();
				}
				buffer.put(key, fs);
			}
			
			// XXX: replace LogMap to more proper type
			for (AggregationFunction f: fs) {
				f.apply(new LogMap(log.getParams()));
			}
			
			// flush
			if (needFlush())
				flush();
		} catch(Throwable t) {
			throw new IllegalStateException("logger-name: " + logger.getName() + ", log: " + log.toString(), t);
		}
		
	}

	public void setForceFlush() {
		forceFlush = true;
	}
}
