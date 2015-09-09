package org.araqne.logdb.cep.engine;


public class JedisTest {

//	public static final String CHANNEL_NAME = "commonChannel";
//	final byte[] bfoo = { 0x01, 0x02, 0x03, 0x04 };
//	final byte[] bfoo1 = { 0x01, 0x02, 0x03, 0x04, 0x0A };
//	final byte[] bfoo2 = { 0x01, 0x02, 0x03, 0x04, 0x0B };
//	final byte[] bfoo3 = { 0x01, 0x02, 0x03, 0x04, 0x0C };
//	final byte[] bbar = { 0x05, 0x06, 0x07, 0x08 };
//	final byte[] bbar1 = { 0x05, 0x06, 0x07, 0x08, 0x0A };
//	final byte[] bbar2 = { 0x05, 0x06, 0x07, 0x08, 0x0B };
//	final byte[] bbar3 = { 0x05, 0x06, 0x07, 0x08, 0x0C };
//
//	final byte[] bfoobar = { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 };
//	final byte[] bfoostar = { 0x01, 0x02, 0x03, 0x04, '*' };
//	final byte[] bbarstar = { 0x05, 0x06, 0x07, 0x08, '*' };
//
//	Jedis jedis;
//
//	@Before
//	public void before() {
//		JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "192.168.133.129", 6379);
//		this.jedis = jedisPool.getResource();
//
//	}
//
//	@Test
//	public void map() {
//		Map<String, String> contexts = new ConcurrentHashMap<String, String>();
//		contexts.put(1 + "", null + "");
//
//	}
//
//	@Test
//	public void closeTest() throws InterruptedException {
//		JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "192.168.133.129", 6379);
//		Jedis jedis = jedisPool.getResource();
//		jedis.set("a", "b");
//
//		jedisPool = new JedisPool(new JedisPoolConfig(), "192.168.133.130", 6379);
//		try {
//			jedis = jedisPool.getResource();
//		} catch (Exception e) {
//		}
//		System.out.println(jedis.get("a"));
//	}
//
//	@Test
//	public void pipetranTest() throws InterruptedException {
//		JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "192.168.133.129", 6379);
//		Jedis jedis = jedisPool.getResource();
//
//		Pipeline p = jedis.pipelined();
//
//		p.multi();
//		p.set("aa", "bb");
//		p.sync();
//		;
//		p.exec();
//
//		// Thread.sleep(1000);
//		// String b = jedis.get("aa");
//		// System.out.println("aa is " + b);
//
//		jedisPool.returnResource(jedis);
//		jedisPool.destroy();
//
//	}
//
//	@Test
//	public void myTest() {
//		JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "192.168.133.129", 6379);
//		Jedis jedis = jedisPool.getResource();
//
//		jedis.set("foo", "314");
//		jedis.set("bar", "foo");
//		jedis.set("hello", "world");
//
//		Pipeline p = jedis.pipelined();
//		p.multi();
//		Response<String> r1 = p.get("bar");
//		Response<String> r2 = p.get("foo");
//		Response<String> r3 = p.get("hello");
//		p.exec();
//		p.sync();
//
//		// before multi
//		assertEquals("foo", r1.get());
//		// It should be readable whether exec's response was built or not
//		assertEquals("314", r2.get());
//		// after multi
//		assertEquals("world", r3.get());
//	}
//
//	@Test
//	public void multiWithSync() {
//		jedis.set("foo", "314");
//		jedis.set("bar", "foo");
//		jedis.set("hello", "world");
//		Pipeline p = jedis.pipelined();
//		Response<String> r1 = p.get("bar");
//
//		p.multi();
//		Response<String> r2 = p.get("foo");
//		p.exec();
//		Response<String> r3 = p.get("hello");
//		p.sync();
//
//		// before multi
//		assertEquals("foo", r1.get());
//		// It should be readable whether exec's response was built or not
//		assertEquals("314", r2.get());
//		// after multi
//		assertEquals("world", r3.get());
//	}
//
//	@Test
//	public void treadhTest() {
//		ExecutorService executor = Executors.newFixedThreadPool(100);
//
//		List<Callable<Long>> tasks = new ArrayList<Callable<Long>>();
//		tasks.add(new Callable<Long>() {
//
//			@Override
//			public Long call() {
//				try {
//					Pipeline p = jedis.pipelined();
//					for (int i = 0; i < 1000; i++) {
//						p.multi();
//						Response<String> a = p.get("a");
//						p.exec();
//						p.set("a", "b" + "a");
//
//					}
//					p.sync();
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//				return 0L;
//			}
//		});
//
//		try {
//			executor.invokeAll(tasks);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//
//		System.out.println("test");
//
//	}
//
//	private class T1 implements Runnable {
//
//		Jedis jedis = null;
//
//		public T1(Jedis jedis) {
//			this.jedis = jedis;
//		}
//
//		@Override
//		public void run() {
//			Pipeline p = jedis.pipelined();
//			for (int i = 0; i < 1000; i++) {
//				p.multi();
//				Response<String> a = p.get("a");
//				p.set("a", a.get());
//				p.exec();
//
//			}
//			p.sync();
//		}
//
//	}
//
//	//@Test
//	public void scan2() {
//		JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "192.168.133.129", 6379);
//		Jedis jedis = jedisPool.getResource();
//
//		for (int i = 0; i < 30; i++) {
//			jedis.set("a" + i, "b" + i);
//		}
//
//		// jedis.set("a".getBytes(), "b".getBytes());
//		// jedis.set("a1".getBytes(), "b1".getBytes());
//		// jedis.set("a2".getBytes(), "b2".getBytes());
//
//		// System.out.println(new String(jedis.get("a1".getBytes())));
//		ScanParams params = new ScanParams();
//		String keyPrefix = "*";
//		// String keyPrefix = name + ":" + id + ":*";
//		// params.match(keyPrefix);
//		params.count(20);
//
//		List<String> searched = new ArrayList<String>();
//
//		byte[] cursor = ScanParams.SCAN_POINTER_START_BINARY;
//
//		// System.out.println(new String(jedis.get("a1".getBytes())));
//		int i = 0;
//		int j = 0;
//		while (true) {
//			ScanResult<byte[]> result = jedis.scan(cursor, params);
//			cursor = result.getCursorAsBytes();
//
//			for (byte[] bkey : result.getResult()) {
//				try {
//
//					searched.add(new String(bkey));
//
//					System.out.println(++i + new String(bkey) + result.getResult().size());
//
//				} catch (Throwable t) {
//					t.printStackTrace();
//				}
//			}
//
//			if (Arrays.equals(result.getCursorAsBytes(), ScanParams.SCAN_POINTER_START_BINARY))
//				break;
//		}
//
//		System.out.println(j);
//		Collections.sort(searched);
//		System.out.println(searched);
//		System.out.println(searched.size());
//	}
//
//	// @Test
//	public void scan() {
//		JedisPool pool = new JedisPool(new JedisPoolConfig(), "192.168.133.129", 6379, 10000);
//		Jedis jedis = pool.getResource();
//
//		jedis.set("b", "b");
//		jedis.set("a", "a");
//
//		ScanResult<String> result = jedis.scan(SCAN_POINTER_START);
//
//		System.out.println(result.getResult());
//
//		assertFalse(result.getResult().isEmpty());
//
//		// binary
//		ScanResult<byte[]> bResult = jedis.scan(SCAN_POINTER_START_BINARY);
//
//		System.out.println(bResult.getResult());
//		assertArrayEquals(SCAN_POINTER_START_BINARY, bResult.getCursorAsBytes());
//		assertFalse(bResult.getResult().isEmpty());
//	}
//
//	@Test
//	public void scanCount() {
//		JedisPool pool = new JedisPool(new JedisPoolConfig(), "192.168.133.129", 6379, 10000);
//		Jedis jedis = pool.getResource();
//
//		ScanParams params = new ScanParams();
//		params.match("a");
//		params.count(2);
//
//		for (int i = 0; i < 10; i++) {
//			jedis.set("a" + i, "a" + i);
//		}
//
//		ScanResult<String> result = jedis.scan(SCAN_POINTER_START, params);
//
//		// assertFalse(result.getResult().isEmpty());
//
//		// binary
//		params = new ScanParams();
//		params.count(2);
//
//		jedis.set(bfoo1, bbar);
//		jedis.set(bfoo2, bbar);
//		jedis.set(bfoo3, bbar);
//
//		ScanResult<byte[]> bResult = jedis.scan(SCAN_POINTER_START_BINARY, params);
//
//		for (byte[] b : bResult.getResult())
//			System.out.println(b);
//
//		// assertFalse(bResult.getResult().isEmpty());
//	}
//
//	// @Test
//	public void scan3() {
//		Set<String> sentinels = new HashSet<String>();
//		sentinels.add("192.168.133.135:26379");
//		JedisSentinelPool jedisPool = new JedisSentinelPool("mymaster", sentinels);
//		// jedisPool.getResource().sentinelSlaves("mymsater");
//
//		Jedis j = jedisPool.getResource();
//
//		j.auth("araqne");
//		Transaction t = j.multi();
//
//		t.set("a4", "b");
//		t.set("a5", "b");
//
//		t.exec();
//
//		// Jedis jedis = new Jedis("192.168.133.135", 26379);//
//		// jedisPool.getResource();
//		// List<Map<String, String>> slaves = jedis.sentinelSlaves("mymaster");
//		//
//		// for(Map<String, String> slave : slaves){
//		// System.out.println(slave.get("ip") + ":" + slave.get("port"));
//		// }
//		//
//		// List<Map<String, String>> masters = jedis.sentinelMasters();
//	}
//
//	@Test
//	public void connectingTest() {
//		try {
//			Jedis jedis2 = new Jedis("192.168.133.134", 6379);
//			jedis2.auth("araqne");
//			System.out.println("connected?");
//			System.out.println(jedis2.isConnected());
//			System.out.println("end");
//			jedis2.close();
//		} catch (Exception e) {
//			e.printStackTrace();
//			jedis.close();
//		}
//
//	}
//
//	@Test
//	public void Test2() {
//		JedisPoolConfig poolConfig = new JedisPoolConfig();
//		JedisPool pool = new JedisPool(poolConfig, "192.168.133.129", 6379, 30000);
//
//		Jedis jc = pool.getResource();
//
//		jc.set("testttt".getBytes(), "tt".getBytes());
//
//		jc.close();
//	}
//
//	@Test
//	public void bytesTest() {
//		try {
//			JedisPoolConfig poolConfig = new JedisPoolConfig();
//			JedisPool pool = new JedisPool(poolConfig, "192.168.133.129", 6379, 30000);
//			Jedis jedis = pool.getResource();
//
//			String key = "key";
//			String value = "value";
//			jedis.set(key.getBytes(), value.getBytes());
//			assertEquals("value", new String(jedis.get("key")));
//			if (jedis != null)
//				pool.returnResource(jedis);
//			if (pool != null) {
//				pool.close();
//				pool.destroy();
//			}
//		} catch (Exception e) {
//		}
//	}

}
