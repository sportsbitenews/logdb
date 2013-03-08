/*
 * Copyright 2011 Future Systems
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
package org.araqne.logdb.query.engine;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.log.api.LogParserFactoryRegistry;
import org.araqne.logdb.AccountService;
import org.araqne.logdb.EmptyLogQueryCallback;
import org.araqne.logdb.LogQuery;
import org.araqne.logdb.LogQueryCommandParser;
import org.araqne.logdb.LogQueryContext;
import org.araqne.logdb.LogQueryEventListener;
import org.araqne.logdb.LogQueryParserService;
import org.araqne.logdb.LogQueryScriptRegistry;
import org.araqne.logdb.LogQueryService;
import org.araqne.logdb.LogQueryStatus;
import org.araqne.logdb.LookupHandlerRegistry;
import org.araqne.logdb.Session;
import org.araqne.logdb.query.parser.DropParser;
import org.araqne.logdb.query.parser.EvalParser;
import org.araqne.logdb.query.parser.FieldsParser;
import org.araqne.logdb.query.parser.JsonParser;
import org.araqne.logdb.query.parser.LookupParser;
import org.araqne.logdb.query.parser.OutputCsvParser;
import org.araqne.logdb.query.parser.RenameParser;
import org.araqne.logdb.query.parser.RexParser;
import org.araqne.logdb.query.parser.ScriptParser;
import org.araqne.logdb.query.parser.SearchParser;
import org.araqne.logdb.query.parser.SortParser;
import org.araqne.logdb.query.parser.StatsParser;
import org.araqne.logdb.query.parser.TableParser;
import org.araqne.logdb.query.parser.TextFileParser;
import org.araqne.logdb.query.parser.TimechartParser;
import org.araqne.logdb.query.parser.ZipFileParser;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogTableRegistry;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name = "logdb-query")
@Provides
public class LogQueryServiceImpl implements LogQueryService {
	private final Logger logger = LoggerFactory.getLogger(LogQueryServiceImpl.class);

	@Requires
	private AccountService accountService;

	@Requires
	private LogStorage logStorage;

	@Requires
	private LogTableRegistry tableRegistry;

	@Requires
	private LookupHandlerRegistry lookupRegistry;

	@Requires
	private LogQueryScriptRegistry scriptRegistry;

	@Requires
	private LogParserFactoryRegistry parserFactoryRegistry;

	@Requires
	private LogQueryParserService queryParserService;

	private BundleContext bc;
	private ConcurrentMap<Integer, LogQuery> queries;

	private CopyOnWriteArraySet<LogQueryEventListener> callbacks;

	private List<LogQueryCommandParser> queryParsers;

	public LogQueryServiceImpl(BundleContext bc) {
		this.bc = bc;
		this.queries = new ConcurrentHashMap<Integer, LogQuery>();
		this.callbacks = new CopyOnWriteArraySet<LogQueryEventListener>();

		// ensure directory
		File dir = new File(System.getProperty("araqne.data.dir"), "araqne-logdb/query");
		dir.mkdirs();

		prepareQueryParsers();
	}

	private void prepareQueryParsers() {
		@SuppressWarnings("unchecked")
		List<Class<? extends LogQueryCommandParser>> parserClazzes = Arrays.asList(DropParser.class, SearchParser.class,
				StatsParser.class, FieldsParser.class, SortParser.class, TimechartParser.class, RenameParser.class,
				EvalParser.class, RexParser.class, JsonParser.class);

		List<LogQueryCommandParser> parsers = new ArrayList<LogQueryCommandParser>();
		for (Class<? extends LogQueryCommandParser> clazz : parserClazzes) {
			try {
				parsers.add(clazz.newInstance());
			} catch (Exception e) {
				logger.error("araqne logdb: failed to add syntax: " + clazz.getSimpleName(), e);
			}
		}

		// add table and lookup (need some constructor injection)
		parsers.add(new TableParser(accountService, logStorage, tableRegistry, parserFactoryRegistry));
		parsers.add(new LookupParser(lookupRegistry));
		parsers.add(new ScriptParser(bc, scriptRegistry));
		parsers.add(new TextFileParser(parserFactoryRegistry));
		parsers.add(new ZipFileParser(parserFactoryRegistry));
		parsers.add(new OutputCsvParser());

		this.queryParsers = parsers;
	}

	@Validate
	public void start() {
		for (LogQueryCommandParser p : queryParsers)
			queryParserService.addCommandParser(p);

		// receive log table event and register it to data source registry
	}

	@Invalidate
	public void stop() {
		if (queryParserService != null) {
			for (LogQueryCommandParser p : queryParsers)
				queryParserService.removeCommandParser(p);
		}
	}

	@Override
	public LogQuery createQuery(Session session, String query) {
		LogQuery lq = queryParserService.parse(new LogQueryContext(session), query);
		queries.put(lq.getId(), lq);
		lq.registerQueryCallback(new EofReceiver(lq));
		invokeCallbacks(lq, LogQueryStatus.Created);

		return lq;
	}

	@Override
	public void startQuery(int id) {
		LogQuery lq = getQuery(id);
		if (lq == null)
			throw new IllegalArgumentException("invalid log query id: " + id);

		new Thread(lq, "Log Query " + id).start();
		invokeCallbacks(lq, LogQueryStatus.Started);
	}

	@Override
	public void removeQuery(int id) {
		LogQuery lq = queries.remove(id);
		if (lq == null) {
			logger.debug("araqne logdb: query [{}] not found, remove failed", id);
			return;
		}

		try {
			lq.clearTimelineCallbacks();
			lq.clearQueryCallbacks();

			if (!lq.isEnd())
				lq.cancel();
		} catch (Throwable t) {
			logger.error("araqne logdb: cannot cancel query " + lq, t);
		}

		try {
			lq.purge();
		} catch (Throwable t) {
			logger.error("araqne logdb: cannot close file buffer list for query " + lq.getId(), t);
		}

		invokeCallbacks(lq, LogQueryStatus.Removed);
	}

	@Override
	public Collection<LogQuery> getQueries() {
		return queries.values();
	}

	@Override
	public LogQuery getQuery(int id) {
		return queries.get(id);
	}

	@Override
	public void addListener(LogQueryEventListener listener) {
		callbacks.add(listener);
	}

	@Override
	public void removeListener(LogQueryEventListener listener) {
		callbacks.remove(listener);
	}

	private void invokeCallbacks(LogQuery lq, LogQueryStatus status) {
		logger.debug("araqne logdb: invoking callback to notify query [{}], status [{}]", lq.getId(), status);
		for (LogQueryEventListener callback : callbacks) {
			try {
				callback.onQueryStatusChange(lq, status);
			} catch (Exception e) {
				logger.warn("araqne logdb: query event listener should not throw any exception", e);
			}
		}
	}

	private class EofReceiver extends EmptyLogQueryCallback {
		private LogQuery query;

		public EofReceiver(LogQuery query) {
			this.query = query;
		}

		@Override
		public void onEof() {
			invokeCallbacks(query, LogQueryStatus.Eof);
		}
	}
}
