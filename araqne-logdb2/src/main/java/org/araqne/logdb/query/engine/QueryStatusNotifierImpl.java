package org.araqne.logdb.query.engine;

import java.io.IOException;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logdb.Query;
import org.araqne.logdb.QueryResultCallback;
import org.araqne.logdb.QueryService;
import org.araqne.logdb.QueryStatusCallback;
import org.araqne.logdb.QueryStatusNotifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name = "query-status-notifier")
@Provides
public class QueryStatusNotifierImpl implements QueryStatusNotifier {
	private final Logger logger = LoggerFactory.getLogger(QueryStatusNotifierImpl.class);

	@Requires
	private QueryService queryService;

	private volatile boolean doStop;

	private Monitor monitor;

	@Validate
	public void start() {
		monitor = new Monitor();
		monitor.start();
	}

	@Invalidate
	public void stop() {
		doStop = true;
		monitor.interrupt();
	}

	private class Monitor extends Thread {
		@Override
		public void run() {
			try {
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
			long count = 0;
			try {
				count = q.getResultCount();
			} catch (IOException e) {
				return;
			}

			for (QueryStatusCallback c : q.getCallbacks().getStatusCallbacks()) {
				c.onChange(q);
			}

			for (QueryResultCallback c : q.getCallbacks().getResultCallbacks()) {
				if (c.offset() + c.limit() <= count)
					c.onPageLoaded(q);
			}
		}
	}
}
