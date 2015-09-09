//package org.araqne.logdb.cep.engine;
//
//import java.nio.ByteBuffer;
//import java.util.ArrayList;
//import java.util.Date;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.Callable;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//
//import org.araqne.codec.FastEncodingRule;
//import org.araqne.logdb.cep.EventContext;
//import org.araqne.logdb.cep.EventKey;
//
//import redis.clients.jedis.Jedis;
//import redis.clients.jedis.JedisPool;
//import redis.clients.jedis.JedisPoolConfig;
// 
//public class RedisTest {
//	static int threadCnt = 4;
//	static int countPerThread = 2000;
//	static JedisPool jedisPool;
//	public static void main(String[] args) throws InterruptedException {
//		ExecutorService pool = Executors.newFixedThreadPool(threadCnt);
//		jedisPool =  new JedisPool(new JedisPoolConfig(), "localhost", 6379, 30000);
//		{
//			Jedis jedis = jedisPool.getResource();
//			jedis.flushAll();
//			jedis.close();
//		}
// 
//		List<Callable<Long>> tasks = new ArrayList<Callable<Long>>(threadCnt);
//		for (int i = 0; i < threadCnt; ++i) {
//			final Jedis jedis = jedisPool.getResource();
//			final int tid = i;
// 
//			tasks.add(new Callable<Long>() {
//				@Override
//				public Long call() throws Exception {
//					perThreadTask(tid, jedis);
//					return 0L;
//				}
//			});
//		}
//		
//		long started = milliTick();
//		pool.invokeAll(tasks);
//		long elapsed = milliTick() - started;
//		System.out.println("total: " + elapsed);
//		System.out.println("total: " + (double) countPerThread * threadCnt / elapsed * 1000.0f + " eps");
//		{
//			Jedis jedis = new Jedis("localhost");
//			System.out.println(jedis.dbSize());
//			jedis.close();
//		}
//	}
//
//	
//	private static void perThreadTask(int tid, Jedis jedis) {
// 
//		EventContext ec = new EventContext(
//				new EventKey("topic", "key"),
//				new Date().getTime(),
//				new Date().getTime(),
//				new Date().getTime(),
//				0,
//				"xeraph");
//		
//		FastEncodingRule rule = new FastEncodingRule();
// 
//		for (int i = 0; i < countPerThread; ++i) {
//			Map<String, Object> m = new HashMap<String, Object>();
//			String rk = ec.getKey().getTopic() + ":" + tid + ec.getKey().getKey();
// 
//			m.put("created", ec.getCreated());
//			m.put("timeout", ec.getTimeoutTime());
//			m.put("expired", ec.getExpireTime());
//			m.put("rows", ec.getRows());
//			m.put("host", ec.getHost());
//			String rki = rk + i;
//			ByteBuffer bb = rule.encode(m);
//			byte[] buf = new byte[bb.remaining()];
//			bb.get(buf);
//		//	jedis.set(rki.getBytes(Charset.forName("utf-8")), buf);
//		//	jedis.get(rki.getBytes(Charset.forName("utf-8")));
//		}
// 
////		long elapsed = milliTick() - started;
////		System.out.println(elapsed);
////		System.out.println((double) count/ elapsed * 1000.0f + " eps");
//	}
// 
//	private static long milliTick() {
//		return System.nanoTime() / 1000000L;
//	}
//}