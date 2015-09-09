/*
 * Copyright 2015 Eediom Inc.
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
package org.araqne.logdb.cep.engine;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.codec.EncodingRule;
import org.araqne.logdb.Row;
import org.araqne.logdb.cep.Event;
import org.araqne.logdb.cep.EventCause;
import org.araqne.logdb.cep.EventClock;
import org.araqne.logdb.cep.EventClockCallback;
import org.araqne.logdb.cep.EventClockItem;
import org.araqne.logdb.cep.EventClockSimpleItem;
import org.araqne.logdb.cep.EventContext;
import org.araqne.logdb.cep.EventContextListener;
import org.araqne.logdb.cep.EventContextService;
import org.araqne.logdb.cep.EventContextStorage;
import org.araqne.logdb.cep.EventKey;
import org.araqne.logdb.cep.EventSubscriber;
import org.araqne.logdb.cep.redis.RedisConfig;
import org.araqne.logdb.cep.redis.RedisConfigRegistry;
import org.araqne.logdb.cep.redis.RedisConfigRegistryListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.Pool;

@Component(name = "redis-event-ctx-storage")
public class RedisEventContextStorage implements EventContextStorage, EventContextListener, RedisConfigRegistryListener {
	private final Logger slog = LoggerFactory.getLogger(RedisEventContextStorage.class);

	@Requires
	private EventContextService eventContextService;

	@Requires
	private RedisConfigRegistry configReg;

	// topic to subscribers
	private ConcurrentHashMap<String, CopyOnWriteArraySet<EventSubscriber>> subscribers;

	private Pool<Jedis> jedisPool;

	private Jedis jedis;
	private Jedis jedisForScan;

	private Object jedisLock = new Object();
	private Object jedisScanLock = new Object();
	private Object theadLock = new Object();

	Map<HostAndPort, Jedis> aliveJedisServers = new ConcurrentHashMap<HostAndPort, Jedis>();

	final static String keyPrefix = "evxkey:";
	final static String expireKeyPrefix = "eprkey:";

	volatile static boolean subscribeStopRequested = false;

	private ConcurrentHashMap<String, EventClock<EventClockSimpleItem>> logClocks;
	private ConcurrentHashMap<EventKey, EventClockSimpleItem> logClockItems;

	private final int retryCnt = 3;

	@Override
	public String getName() {
		return "redis";
	}

	@Validate
	public void start() {
		try {
			if (!redisMode())
				return;

			configReg.addListener(this);
			subscribers = new ConcurrentHashMap<String, CopyOnWriteArraySet<EventSubscriber>>();
			logClocks = new ConcurrentHashMap<String, EventClock<EventClockSimpleItem>>();
			logClockItems = new ConcurrentHashMap<EventKey, EventClockSimpleItem>();

			synchronized (theadLock) {
				theadLock.notify();
				subscribeStopRequested = false;
			}

			connect();

			if (jedis != null)
				jedis.flushDB();

		} catch (Exception e) {
			slog.error("araqne logdb cep: failed to start redis storage", e);
		}

		new Thread(new Runnable() {
			@Override
			public void run() {
				try {

					ExecutorService executor = Executors.newCachedThreadPool();// FixedThreadPool(10);

					synchronized (theadLock) {
						while (!subscribeStopRequested) {
							for (HostAndPort server : registerServers()) {
								Jedis jedisForSub = null;
								try {

									if (aliveJedisServers.keySet().contains(server))
										continue;

									jedisForSub = new Jedis(server.getHost(), server.getPort());
									Psub subscribeRegister = new Psub(server);
									aliveJedisServers.put(server, jedisForSub);

									executor.submit(subscribeRegister);

								} catch (Exception e) {
									closeClient(jedisForSub);
									aliveJedisServers.remove(server);
									slog.warn("araqne logdb cep: cannot start subscribe client [" + server.toString() + "]", e);
								}
							}
							theadLock.wait(1000);
						}
					}
					executor.shutdownNow();
				} catch (Exception e) {
					slog.error("araqne logdb cep: failed to start redis expire monitoring service", e);
				}
			}
		}, "Subcribing Redis Client").start();

		eventContextService.registerStorage(this);
	}

	@Invalidate
	public void stop() {
		if (!redisMode())
			return;

		synchronized (theadLock) {
			theadLock.notify();
			subscribeStopRequested = true;
		}

		if (eventContextService != null)
			eventContextService.unregisterStorage(this);
		configReg.removeListener(this);

		for (Jedis aliveJedis : aliveJedisServers.values()) {
			closeClient(aliveJedis);
		}

		aliveJedisServers.clear();

		try {
			returnResource(jedis);
			returnResource(jedisForScan);
			closeClient(jedis);
			closeClient(jedisForScan);

			if (jedisPool != null) {
				jedisPool.close();
				jedisPool.destroy();
				jedisPool = null;
			}
		} catch (Exception e) {
			slog.warn("araqne logdb cep: failed to close redis storage", e);
		}
	}

	private boolean redisMode() {
		String engine = System.getProperty("araqne.logdb.cepengine");
		if (engine == null || engine.isEmpty() || !engine.equals("redis"))
			return false;

		return true;
	}

	private void connect() {

		try {
			returnResource(jedis);
			returnResource(jedisForScan);
			if (jedisPool != null)
				jedisPool.destroy();
		} catch (Exception e) {
			slog.info("araqne logdb cep: cannot return resource jedis client", e);
		} finally {
			jedis = null;
			jedisForScan = null;
			jedisPool = null;
		}

		jedisPool = getJedisPool();
		jedis = jedisPool.getResource();
		jedisForScan = jedisPool.getResource();

		String password = configReg.getConfig().getPassword();
		if (password != null && !password.trim().isEmpty()) {
			jedis.auth(password);
			jedisForScan.auth(password);
		}
	}

	private List<HostAndPort> registerServers() {
		List<HostAndPort> servers = new ArrayList<HostAndPort>();
		RedisConfig config = configReg.getConfig();

		if (config.isSentinel()) {
			Jedis sentinel = new Jedis(config.getHost(), config.getPort());
			try {
				List<Map<String, String>> slaves = sentinel.sentinelSlaves(config.getSentinelName());
				for (Map<String, String> slave : slaves) {
					String host = slave.get("ip");
					int port = Integer.parseInt(slave.get("port"));
					servers.add(new HostAndPort(host, port));
					if (slog.isDebugEnabled())
						slog.debug("araqne logdb cep: registered redis sentinel server [{}:{}]", host, port);
				}
			} finally {
				sentinel.close();
			}

			Set<String> sentinels = new HashSet<String>();
			sentinels.add(config.getHost() + ":" + config.getPort());
			servers.add(((JedisSentinelPool) jedisPool).getCurrentHostMaster());

		} else {
			servers.add(new HostAndPort(config.getHost(), config.getPort()));
		}

		return servers;
	}

	private Pool<Jedis> getJedisPool() {
		RedisConfig config = configReg.getConfig();
		if(config == null) {
			slog.error("araqne logdb cep: redis config does not exist");
			throw new IllegalArgumentException("redis config does not exist");
		}
		
		String host = config.getHost();
		int port = config.getPort();

		if (config.isSentinel()) {
			Set<String> sentinels = new HashSet<String>();
			sentinels.add(host + ":" + port);
			if (slog.isDebugEnabled())
				slog.debug("araqne logdb cep: conntecting jedis sentinel pool [{}:{}]", host, port);
			return new JedisSentinelPool(config.getSentinelName(), sentinels);
		} else {
			if (slog.isDebugEnabled())
				slog.debug("araqne logdb cep: connecting jedis pool [{}:{}]", host, port);

			return new JedisPool(new JedisPoolConfig(), host, port, 10000);
		}
	}

	@Override
	public Set<String> getHosts() {
		return new HashSet<String>(logClocks.keySet());
	}

	@Override
	public EventClock<? extends EventClockItem> getClock(String host) {
		return logClocks.get(host);
	}

	@Override
	public Iterator<EventKey> getContextKeys() {
		return getContextKeys("*");
	}

	@Override
	public Iterator<EventKey> getContextKeys(String topic) {
		return new RedisScanIterator(topic);
	}

	@Override
	public EventContext getContext(EventKey ctx) {
		Response<byte[]> value = null;
		RuntimeException lastException = null;
		synchronized (jedisLock) {
			for (int i = 0; i < retryCnt; ++i) {
				lastException = null;
				try {
					if (jedis == null)
						connect();

					Transaction t = jedis.multi();
					value = t.get(redisKey(ctx));
					t.exec();
					break;
				} catch (JedisConnectionException e) {
					returnResource(jedis);
					closeClient(jedis);
					jedis = null;
					lastException = e;
					slog.debug("araqne logdb cep: cannot get event context [{}]", ctx);
				}
			}
		}

		if (lastException != null)
			throw lastException;

		byte[] byteValue = value.get();
		if (byteValue == null)
			return null;
		else
			return parseContext(byteValue);
	}

	private static String expireKey(EventKey key) {
		return expireKeyPrefix + EventKey.marshal(key);
	}

	private static byte[] redisKey(EventKey key) {
		return (keyPrefix + EventKey.marshal(key)).getBytes();
	}

	private static EventKey parseKey(byte[] b) {
		String key = new String(b);
		return EventKey.parse(key.substring(keyPrefix.length()));
	}

	private static EventKey parseExpireKey(String key) {
		return EventKey.parse(key.substring(expireKeyPrefix.length()));
	}

	private static byte[] redisValue(EventContext ctx) {
		ByteBuffer bb = ByteBuffer.allocate(EncodingRule.lengthOf(ctx.marshal()));
		EncodingRule.encode(bb, ctx.marshal());
		return bb.array();
	}

	@SuppressWarnings("unchecked")
	private static EventContext parseContext(byte[] b) {
		return EventContext.parse((Map<String, Object>) decode(b));
	}

	@Override
	public void advanceTime(String host, long logTime) {
		EventClock<EventClockSimpleItem> logClock = ensureClock(logClocks, host, logTime);
		logClock.setTime(logTime, false);
	}

	@Override
	public void clearClocks() {
		Set<EventClockSimpleItem> items = new HashSet<EventClockSimpleItem>();

		for (EventClock<EventClockSimpleItem> logClock : logClocks.values()) {
			for (EventClockSimpleItem item : logClock.getExpireContexts())
				items.add(item);

			for (EventClockSimpleItem item : logClock.getTimeoutContexts())
				items.add(item);
		}

		RuntimeException lastException = null;
		synchronized (jedisLock) {
			for (int i = 0; i < retryCnt; ++i) {
				lastException = null;
				try {
					if (jedis == null)
						connect();

					Transaction t = jedis.multi();
					for (EventClockSimpleItem item : items) {
						EventKey key = item.getKey();
						t.del(redisKey(key));
						t.del(expireKey(key));
					}

					t.exec();

					break;
				} catch (JedisConnectionException e) {
					returnResource(jedis);
					closeClient(jedis);
					jedis = null;
					lastException = e;
					slog.debug("araqne logdb cep: cannot clear redis clocks - failed to delete");
				}
			}
		}

		if (lastException != null)
			throw lastException;

		logClocks = new ConcurrentHashMap<String, EventClock<EventClockSimpleItem>>();
		logClockItems = new ConcurrentHashMap<EventKey, EventClockSimpleItem>();
	}

	@Override
	public void clearContexts() {
		clearContexts("*");
	}

	@Override
	public void clearContexts(String topic) {
		Iterator<EventKey> itr = getContextKeys(topic);

		RuntimeException lastException = null;
		synchronized (jedisLock) {
			for (int i = 0; i < retryCnt; ++i) {
				lastException = null;
				try {
					if (jedis == null)
						connect();

					Transaction t = jedis.multi();

					while (itr.hasNext()) {
						EventKey key = itr.next();
						t.del(redisKey(key));
						t.del(expireKey(key));
					}

					t.exec();

					return;
				} catch (JedisConnectionException e) {
					returnResource(jedis);
					closeClient(jedis);
					jedis = null;
					lastException = e;
					slog.debug("araqne logdb cep: cannot clear redis contexts - failed to delete");
				}
			}
		}

		if (lastException != null)
			throw lastException;
		else
			return;
	}

	@Override
	public void addSubscriber(String topic, EventSubscriber subscriber) {
		CopyOnWriteArraySet<EventSubscriber> s = new CopyOnWriteArraySet<EventSubscriber>();
		CopyOnWriteArraySet<EventSubscriber> old = subscribers.putIfAbsent(topic, s);
		if (old != null)
			s = old;

		s.add(subscriber);
	}

	@Override
	public void removeSubscriber(String topic, EventSubscriber subscriber) {
		CopyOnWriteArraySet<EventSubscriber> s = subscribers.get(topic);
		if (s != null)
			s.remove(subscriber);
	}

	private void generateEvent(EventContext ctx, EventCause cause) {
		if (slog.isDebugEnabled())
			slog.debug("araqne logdb cep: generate event ctx [{}] cause [{}]", ctx.getKey(), cause);

		if (cause == EventCause.EXPIRE) {
			if (ctx.getTimeoutTime() != 0) {
				if (ctx.getExpireTime() == 0 || ctx.getTimeoutTime() < ctx.getExpireTime())
					cause = EventCause.TIMEOUT;
			}
		}

		Event ev = new Event(ctx, cause);
		ev.getRows().addAll(ctx.getRows());

		CopyOnWriteArraySet<EventSubscriber> s = subscribers.get(ctx.getKey().getTopic());
		if (s != null) {
			for (EventSubscriber subscriber : s) {
				try {
					subscriber.onEvent(ev);
				} catch (Throwable t) {
					slog.error("araqne logdb cep: subscriber should not throw any exception", t);
				}
			}
		}

		// for wild subscriber
		s = subscribers.get("*");
		if (s != null) {
			for (EventSubscriber subscriber : s) {
				try {
					subscriber.onEvent(ev);
				} catch (Throwable t) {
					slog.error("araqne logdb cep: subscriber should not throw any exception", t);
				}
			}
		}
	}

	@Override
	public void onUpdateTimeout(EventContext ctx) {
	}

	@Override
	public EventContext addContext(EventContext ctx) {
		byte[] oldByteValue = null;
		RuntimeException lastException = null;
		synchronized (jedisLock) {
			for (int i = 0; i < retryCnt; ++i) {
				lastException = null;
				try {
					if (jedis == null)
						connect();

					Transaction t = jedis.multi();
					Response<byte[]> pipeString = t.get(redisKey(ctx.getKey()));
					t.exec();

					t = jedis.multi();
					oldByteValue = pipeString.get();

					if (oldByteValue == null)
						return null;

					@SuppressWarnings("unchecked")
					EventContext oldCtx = EventContext.parse((Map<String, Object>) decode(oldByteValue));
					ctx = contextMerge(oldCtx, ctx);
					t.set(redisKey(ctx.getKey()), redisValue(ctx));
					t.exec();
					break;
				} catch (JedisConnectionException e) {
					returnResource(jedis);
					closeClient(jedis);
					jedis = null;
					lastException = e;
					slog.debug("araqne logdb cep: cannot add redis context [{}]", ctx);
				}
			}
		}

		if (lastException != null)
			throw lastException;

		return ctx;
	}

	@Override
	public void registerContext(EventContext ctx) {
		List<EventContext> contexts = new ArrayList<EventContext>();
		contexts.add(ctx);
		registerContexts(contexts);
	}

	// private List<EventContext> preMergeContexts(List<EventContext> contexts)
	// {
	// Map<EventKey, EventContext> map = new HashMap<EventKey, EventContext>();
	//
	// for (EventContext ctx : contexts) {
	// if (!map.containsKey(ctx.getKey())) {
	// map.put(ctx.getKey(), ctx);
	// } else {
	// contextMerge(map.get(ctx.getKey()), ctx);
	// }
	// }
	//
	// return new ArrayList<EventContext>(map.values());
	// }

	@Override
	public void registerContexts(List<EventContext> contexts) {
		// 한 rowBatch에 중복되는 key가 많이 있을 때 메모리에서 미리 합쳐서 레디스 접속 횟수를 줄여 성능 향상 기대
		// contexts = preMergeContexts(contexts);

		ConcurrentHashMap<EventKey, Response<byte[]>> responses = new ConcurrentHashMap<EventKey, Response<byte[]>>();
		RuntimeException lastException = null;
		synchronized (jedisLock) {
			for (int i = 0; i < retryCnt; ++i) {
				lastException = null;
				try {
					if (jedis == null)
						connect();

					Transaction t = jedis.multi();
					for (EventContext ctx : contexts) {
						Response<byte[]> pipeString = t.get(redisKey(ctx.getKey()));
						responses.put(ctx.getKey(), pipeString);
					}
					t.exec();

					t = jedis.multi();
					for (EventContext ctx : contexts) {
						byte[] oldByteValue = responses.get(ctx.getKey()).get();
						addContextToRedis(ctx, oldByteValue, t);
					}
					t.exec();

					break;
				} catch (JedisConnectionException e) {
					returnResource(jedis);
					closeClient(jedis);
					jedis = null;
					lastException = e;
					slog.debug("araqne logdb cep: cannot add redis contexts");
				}
			}
		}

		if (lastException != null)
			throw lastException;

		for (EventContext ctx : contexts) {
			generateEvent(ctx, EventCause.CREATE);

			if (ctx.getHost() != null) {
				Response<byte[]> oldValue = responses.get(ctx.getKey());
				addHostItem(ctx, oldValue.get());
			}
		}
	}

	private void addHostItem(EventContext ctx, byte[] oldByteValue) {
		if (oldByteValue == null) {
			EventClock<EventClockSimpleItem> logClock = ensureClock(logClocks, ctx.getHost(), ctx.getCreated());
			EventClockSimpleItem item = EventClockSimpleItem.newInstance(ctx);
			logClockItems.put(ctx.getKey(), item);
			logClock.add(item);

		} else if (ctx.getTimeoutTime() != 0) {
			EventClockSimpleItem item = null;
			item = logClockItems.get(ctx.getKey());

			if (item == null) {
				item = EventClockSimpleItem.newInstance(ctx);
				item = logClockItems.putIfAbsent(ctx.getKey(), item);
			}

			EventClock<EventClockSimpleItem> logClock = ensureClock(logClocks, ctx.getHost(), ctx.getCreated());
			item.setTimeoutTime(ctx.getTimeoutTime());
			logClock.updateTimeout(item);
		}
	}

	private void addContextToRedis(EventContext ctx, byte[] oldByteValue, Transaction t) {
		EventKey key = ctx.getKey();
		long timeoutTime = ctx.getTimeoutTime();

		if (oldByteValue != null) {
			@SuppressWarnings("unchecked")
			EventContext oldCtx = EventContext.parse((Map<String, Object>) decode(oldByteValue));

			if (timeoutTime != 0L && ctx.getHost() == null)
				t.pexpireAt(expireKey(key), timeoutTime);

			ctx = contextMerge(oldCtx, ctx);
			t.set(redisKey(key), redisValue(ctx));
		} else {
			t.set(expireKey(key), "");
			t.set(redisKey(key), redisValue(ctx));

			if (ctx.getHost() == null) {
				long redisExpire = getMinValue(timeoutTime, ctx.getExpireTime());
				if (redisExpire != 0)
					t.pexpireAt(expireKey(key), redisExpire);
			}
		}
	}

	private EventContext contextMerge(EventContext oldCtx, EventContext ctx) {
		if (!oldCtx.getKey().equals(ctx.getKey()))
			throw new IllegalArgumentException("event key is not matched [" + oldCtx.toString() + ", " + ctx.toString() + "]");

		if (ctx.getTimeoutTime() != 0L)
			oldCtx.setTimeoutTime(ctx.getTimeoutTime());

		for (Row row : ctx.getRows()) {
			oldCtx.addRow(row);
		}

		for (String vKey : ctx.getVariables().keySet())
			oldCtx.setVariable(vKey, ctx.getVariable(vKey));

		oldCtx.getCounter().addAndGet(ctx.getCounter().get());

		return oldCtx;
	}

	/**
	 * @return a smaller number except 0.
	 */
	private long getMinValue(long a, long b) {
		long min = Math.min(a, b);

		if (min > 0)
			return min;
		else
			return Math.max(a, b);
	}

	private EventClock<EventClockSimpleItem> ensureClock(ConcurrentHashMap<String, EventClock<EventClockSimpleItem>> clocks,
			String host, long time) {
		EventClock<EventClockSimpleItem> clock = null;
		clock = clocks.get(host);
		if (clock == null) {
			clock = new EventClock<EventClockSimpleItem>(new EventClockCallback() {

				@Override
				public void onRemove( EventClockItem value,  EventCause cause) {
					removeContext(value.getKey(), null, cause);
					logClockItems.remove(value.getKey());
				}
			}, host, time, 11);

			EventClock<EventClockSimpleItem> old = clocks.putIfAbsent(host, clock);
			if (old != null)
				return old;
			return clock;
		} else {
			return clock;
		}
	}

	@Override
	public void removeContext(EventKey key, Row row, EventCause cause) {
		Map<EventKey, Row> contexts = new HashMap<EventKey, Row>();
		contexts.put(key, row);
		removeContexts(contexts, cause);
	}

	@Override
	public void removeContexts(Map<EventKey, Row> contexts, EventCause cause) {
		ConcurrentHashMap<EventKey, Response<byte[]>> responses = new ConcurrentHashMap<EventKey, Response<byte[]>>();
		RuntimeException lastException = null;
		synchronized (jedisLock) {
			for (int i = 0; i < retryCnt; ++i) {
				lastException = null;
				try {
					if (jedis == null)
						connect();
					Transaction t = jedis.multi();

					for (EventKey key : contexts.keySet()) {
						Response<byte[]> oldValue = t.get(redisKey(key));
						responses.put(key, oldValue);
						t.del(redisKey(key));
						t.del(expireKey(key));
					}

					t.exec();
					break;
				} catch (JedisConnectionException e) {
					returnResource(jedis);
					closeClient(jedis);
					jedis = null;
					lastException = e;
					slog.debug("araqne logdb cep: cannot remove redis contexts");
				}
			}
		}

		if (lastException != null)
			throw lastException;

		for (EventKey evtkey : contexts.keySet()) {
			byte[] response = responses.get(evtkey).get();

			if (response != null) {
				EventContext oldCtx = parseContext(response);

				if (oldCtx.getHost() != null) {
					EventClock<EventClockSimpleItem> logClock = ensureClock(logClocks, oldCtx.getHost(), oldCtx.getCreated());
					logClock.remove(logClockItems.get(oldCtx.getKey()));
					logClockItems.remove(oldCtx.getKey());
				}
				
				oldCtx.addRow(contexts.get(evtkey));

				generateEvent(oldCtx, cause);
			}
		}
	}

	@Override
	public Map<EventKey, EventContext> getContexts(Set<EventKey> keys) {
		Map<EventKey, EventContext> contexts = new HashMap<EventKey, EventContext>();
		ConcurrentHashMap<EventKey, Response<byte[]>> responses = new ConcurrentHashMap<EventKey, Response<byte[]>>();
		RuntimeException lastException = null;

		synchronized (jedisLock) {
			for (int i = 0; i < retryCnt; ++i) {
				lastException = null;
				try {
					if (jedis == null)
						connect();

					Transaction t = jedis.multi();

					for (EventKey evtkey : keys) {
						Response<byte[]> value = t.get(redisKey(evtkey));
						responses.put(evtkey, value);
					}
					t.exec();

					break;
				} catch (JedisConnectionException e) {
					returnResource(jedis);
					closeClient(jedis);
					jedis = null;
					lastException = e;
					slog.debug("araqne logdb cep: cannot get redis contexts");
				}
			}
		}

		if (lastException != null)
			throw lastException;

		for (EventKey evtkey : keys) {
			byte[] response = responses.get(evtkey).get();
			if (response != null) {

				@SuppressWarnings("unchecked")
				EventContext ctx = EventContext.parse((Map<String, Object>) decode(response));
				contexts.put(evtkey, ctx);
			} else {
				contexts.put(evtkey, null);
			}
		}

		return contexts;
	}

	@Override
	public void onChanged() {
		connect();
	}

	private void returnResource(Jedis jedis) {
		if (jedis != null) {
			try {
				jedisPool.returnResource(jedis);
			} catch (Exception e) {
				slog.debug("araqne logdb cep: cannot return resource", e);
			}
		}
	}

	private void closeClient(Jedis jedis) {
		if (jedis != null)
			try {
				jedis.disconnect();
				jedis.close();
			} catch (Exception e) {
				slog.debug("araqne logdb cep: cannot close client", e);
			}
	}

	public static Object decode(byte[] bytes) {
		return EncodingRule.decode(ByteBuffer.wrap(bytes));
	}

	private class Psub implements Callable<Long> {
		HostAndPort server;

		private Psub(HostAndPort server) {
			this.server = server;
		}

		@Override
		public Long call() throws Exception {
			RedisConfig config = configReg.getConfig();
			Jedis registeredJedis = aliveJedisServers.get(server);

			try {
				String password = config.getPassword();
				if (password != null && !password.trim().isEmpty()) {
					registeredJedis.auth(password);
				}

				/* redis event 수신 모드 on */
				registeredJedis.configSet("notify-keyspace-events", "AKE");

				registeredJedis.psubscribe(new ExpireSub(), "__keyevent@0__:expired");
			} catch (Exception e) {
				closeClient(registeredJedis);
				aliveJedisServers.remove(server);
				slog.error("araqne logdb cep: disconnected subscribe client [" + server + "]", e);
			}
			return 0L;
		}
	}

	private class ExpireSub extends JedisPubSub {
		@Override
		public void onPMessage(String pattern, String channel, String message) {

			if (!message.startsWith(expireKeyPrefix))
				return;
			EventKey key = null;

			try {
				key = parseExpireKey(message);
				removeContext(key, null, EventCause.EXPIRE);
			} catch (Exception e) {
				slog.debug("araqne logdb cep : event key parse error (" + key + ")", e);
			}
		}
	}

	private class RedisScanIterator implements Iterator<EventKey> {
		final int bufferSize = 5000;

		Set<byte[]> keyBuffer = new HashSet<byte[]>(bufferSize);
		ScanParams params = new ScanParams();
		Iterator<byte[]> bufferIterator = keyBuffer.iterator();;

		byte[] cursor = ScanParams.SCAN_POINTER_START_BINARY;

		public RedisScanIterator(String topic) {
			String filter = keyPrefix + topic + EventKey.delimiter + "*";
			params.match(filter.getBytes());
			params.count(bufferSize);

			scan();
		}

		@Override
		public boolean hasNext() {
			if (Arrays.equals(cursor, ScanParams.SCAN_POINTER_START_BINARY) || keyBuffer.isEmpty()) {
				return bufferIterator.hasNext();
			}
			return true;
		}

		@Override
		public EventKey next() {
			if (bufferIterator == null || bufferIterator.hasNext() == false) {
				scan();
			}
			return parseKey(bufferIterator.next());
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		private void scan() {
			keyBuffer.clear();
			RuntimeException lastException = null;
			ScanResult<byte[]> result = null;

			synchronized (jedisScanLock) {
				for (int i = 0; i < retryCnt; ++i) {
					lastException = null;
					try {
						if (jedisForScan == null)
							connect();

						result = jedisForScan.scan(cursor, params);
						break;
					} catch (JedisConnectionException e) {
						returnResource(jedisForScan);
						closeClient(jedisForScan);
						jedisForScan = null;
						jedis = null;
						lastException = e;
						slog.debug("aranqe logdb cep:cannot scan jedis client", e);
					}
				}
			}

			if (lastException != null)
				throw lastException;

			cursor = result.getCursorAsBytes();

			for (byte[] bkey : result.getResult())
				keyBuffer.add(bkey);

			bufferIterator = keyBuffer.iterator();
		}
	}

}
