package org.araqne.logdb.impl;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logdb.QueryThreadPoolService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name = "logdb-query-thread-pool")
@Provides
public class QueryThreadPoolServiceImpl implements QueryThreadPoolService {
	private ExecutorService executor;

	@Validate
	public void start() {
		executor = Executors.newCachedThreadPool();
	}

	@Invalidate
	public void stop() {
		executor.shutdown();
	}

	@Override
	public void execute(Runnable runnable) {
		execute(runnable, null);
	}

	@Override
	public void execute(Runnable runnable, String tag) {
		executor.execute(new NamedRunnable(runnable, tag));
	}

	@Override
	public <T> Future<T> submit(Callable<T> callable, String tag) {
		return executor.submit(callable);
	}

	private static class NamedRunnable implements Runnable {
		private Runnable r;
		private String tag;
		private String threadName;

		public NamedRunnable(Runnable r, String tag) {
			this.r = r;
			this.tag = tag;

			if (tag == null)
				this.threadName = "Query Thread Pool";
			else
				this.threadName = "Query Thread Pool [" + tag + "]";
		}

		@Override
		public void run() {
			try {
				Thread.currentThread().setName(threadName);
				r.run();
			} catch (Throwable t) {
				Logger slog = LoggerFactory.getLogger(QueryThreadPoolServiceImpl.class);
				slog.error("araqne logdb: uncaught query thread [" + tag + "] exception", t);
			} finally {
				Thread.currentThread().setName("Query Thread Pool");
			}
		}
	}
}
