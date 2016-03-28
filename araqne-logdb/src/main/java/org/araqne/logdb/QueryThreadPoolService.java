package org.araqne.logdb;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public interface QueryThreadPoolService {
	void execute(Runnable runnable);

	void execute(Runnable runnable, String tag);
	
	<T> Future<T> submit(Callable<T> callable, String tag);
}
