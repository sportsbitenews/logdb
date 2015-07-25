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
package org.araqne.logdb.query.engine;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logdb.Query;
import org.araqne.logdb.QueryService;
import org.araqne.logdb.QueryStatusCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name = "logdb-query-status-notifier")
public class QueryStatusNotifier {
	private final Logger logger = LoggerFactory.getLogger(QueryStatusNotifier.class);

	@Requires
	private QueryService queryService;

	private volatile boolean doStop;

	private Monitor monitor;

	@Validate
	public void start() {
		doStop = false;
		monitor = new Monitor();
		monitor.start();
	}

	@Invalidate
	public void stop() {
		doStop = true;
		monitor.interrupt();
	}

	private class Monitor extends Thread {
		public Monitor() {
			super("Query Status Notifier");
		}

		@Override
		public void run() {
			try {
				logger.info("araqne logdb: query status notifier started");
				while (!doStop) {
					try {
						for (Query q : queryService.getQueries())
							checkQuery(q);

						Thread.sleep(2000);
					} catch (InterruptedException e) {
						logger.debug("araqne logdb: query status notifier interrupted");
					}
				}
			} catch (Throwable t) {
				logger.error("araqne logdb: query status notifier error", t);
			} finally {
				logger.info("araqne logdb: query status notifier stopped");
			}
		}

		private void checkQuery(Query q) {
			for (QueryStatusCallback c : q.getCallbacks().getStatusCallbacks()) {
				try {
					c.onChange(q);
				} catch (Throwable t) {
					logger.warn("araqne logdb: query [" + q.getId() + "] status callback should not throw any exception", t);
				}
			}
		}
	}
}
