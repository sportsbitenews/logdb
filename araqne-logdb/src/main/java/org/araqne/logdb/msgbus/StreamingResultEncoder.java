package org.araqne.logdb.msgbus;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import java.util.zip.Deflater;

import org.araqne.codec.Base64;
import org.araqne.codec.FastEncodingRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingResultEncoder {
	private final Logger slog = LoggerFactory.getLogger(StreamingResultEncoder.class);
	private ThreadPoolExecutor executor;
	private int poolSize;

	public StreamingResultEncoder(String name, int poolSize) {
		if (poolSize < 1)
			throw new IllegalArgumentException("pool size should be positive");

		this.poolSize = poolSize;
		this.executor = new ThreadPoolExecutor(poolSize, poolSize, 10, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(
				poolSize), new NamedThreadFactory(name), new CallerRunsPolicy());

		slog.info("araqne logdb: created encoder thread pool [{}]", poolSize);
	}

	public List<Map<String, Object>> encode(List<Object> rows) throws InterruptedException, ExecutionException {
		int flushSize = (rows.size() + poolSize) / poolSize;
		List<Map<String, Object>> chunks = new ArrayList<Map<String, Object>>();
		List<Future<Map<String, Object>>> futures = new ArrayList<Future<Map<String, Object>>>();

		int total = rows.size();
		int from = 0;
		boolean exit = false;
		while (!exit) {
			int to = from + flushSize;
			if (to >= total) {
				to = total;
				exit = true;
			}

			List<Object> slice = rows.subList(from, to);
			Future<Map<String, Object>> future = executor.submit(new Encoder(slice));
			futures.add(future);

			from = to;
		}

		for (Future<Map<String, Object>> f : futures) {
			do {
				Map<String, Object> chunk = f.get();
				if (chunk != null) {
					chunks.add(chunk);

					if (slog.isDebugEnabled()) {
						int original = (Integer) chunk.get("size");
						int compressed = ((byte[]) chunk.get("bin")).length;
						slog.debug("araqne logdb: compressed chunk size [{}] original size [{}]", compressed, original);
					}
				}
			} while (!f.isDone());
		}

		return chunks;
	}

	public void close() {
		executor.shutdown();
		slog.info("araqne logdb: closed encoder thread pool [{}]", poolSize);
	}

	private class Encoder extends FunctorBase<Map<String, Object>> {
		private List<Object> rows;

		public Encoder(List<Object> rows) {
			super(slog);
			this.rows = rows;
		}

		@Override
		protected Map<String, Object> callSafely() throws Exception {
			Map<String, Object> msg = new HashMap<String, Object>();

			FastEncodingRule enc = new FastEncodingRule();
			ByteBuffer bb = enc.encode(rows);

			Deflater c = new Deflater();
			c.setInput(bb.array(), 0, bb.array().length);
			c.finish();

			ByteBuffer compressed = ByteBuffer.allocate(bb.array().length * 2);
			int compressedSize = c.deflate(compressed.array());
			compressed = ByteBuffer.wrap(Arrays.copyOf(compressed.array(), compressedSize));
			c.end();

			msg.put("size", bb.array().length);
			msg.put("bin", new String(Base64.encode(compressed.array())));
			return msg;
		}
	}

	private class NamedThreadFactory implements ThreadFactory {
		private final String prefix;

		public NamedThreadFactory(String prefix) {
			this.prefix = prefix;
		}

		@Override
		public Thread newThread(Runnable r) {
			return new Thread(r, prefix);
		}
	}

	private abstract class FunctorBase<T> implements Callable<T> {
		private final Logger logger;

		public FunctorBase(Logger logger) {
			this.logger = logger;
		}

		@Override
		public final T call() {
			try {
				return callSafely();
			} catch (Throwable t) {
				if (logger != null)
					logger.error("unexpected error while running Task", t);
				else {
					System.err.println("unexpected error while running Task");
					t.printStackTrace(System.err);
				}
				throw new IllegalStateException(t);
			}
		}

		protected abstract T callSafely() throws Exception;

	}

}
