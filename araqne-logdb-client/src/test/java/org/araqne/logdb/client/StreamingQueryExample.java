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
package org.araqne.logdb.client;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.araqne.logdb.client.LogDbClient;
import org.araqne.logdb.client.LogQuery;
import org.araqne.logdb.client.Row;
import org.araqne.logdb.client.StreamingResultSet;

/**
 * supported since araqne-logdb-client 0.9.1
 */
public class StreamingQueryExample {
	public static void main(String[] args) throws IOException {
		long begin = System.currentTimeMillis();
		LogDbClient client = null;
		int queryId = 0;

		ResultCounter counter = new ResultCounter();

		try {
			client = new LogDbClient();
			client.connect("localhost", 8888, "araqne", "");

			// Pass StreamingResultSet to use streaming API
			queryId = client.createQuery("table window=100m foo", counter);
			client.startQuery(queryId);

			// Main thread should wait until streaming is done
			do {
				counter.latch.await(1, TimeUnit.SECONDS);
			} while (counter.latch.getCount() > 0);

		} catch (InterruptedException e) {
		} finally {
			if (client != null) {
				if (queryId != 0)
					client.removeQuery(queryId);

				client.close();
			}

			long end = System.currentTimeMillis();
			long elapsed = end - begin;
			long throughput = counter.count * 1000 / elapsed;

			System.out.println("Elapsed " + elapsed + " ms, " + throughput + " rows/sec");
		}
	}

	private static class ResultCounter implements StreamingResultSet {
		private int count = 0;
		private CountDownLatch latch = new CountDownLatch(1);

		/**
		 * @param query
		 *            related query object
		 * @param rows
		 *            transferred result rows in order
		 * @param last
		 *            true for last onRows() call, otherwise false
		 */
		@Override
		public void onRows(LogQuery query, List<Row> rows, boolean last) {
			for (Row r : rows)
				System.out.println(r);
			
			count += rows.size();
			System.out.println("Received " + count + " rows");

			// End of result, let's awake main thread
			if (last) {
				System.out.println("received LAST call!!!");
				latch.countDown();
			}
		}
	}
}
