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
import java.util.HashSet;
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
import org.araqne.codec.Base64;
import org.araqne.codec.EncodingRule;
import org.araqne.logdb.cep.Event;
import org.araqne.logdb.cep.EventCause;
import org.araqne.logdb.cep.EventClock;
import org.araqne.logdb.cep.EventContext;
import org.araqne.logdb.cep.EventContextListener;
import org.araqne.logdb.cep.EventContextService;
import org.araqne.logdb.cep.EventContextStorage;
import org.araqne.logdb.cep.EventKey;
import org.araqne.logdb.cep.EventSubscriber;
import org.araqne.logdb.cep.redis.RedisConfig;
import org.araqne.logdb.cep.redis.RedisConfigRegistry;
import org.araqne.logdb.cep.redis.RedisConfigRegistryListener;
import org.araqne.msgbus.Marshalable;
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
public class RedisEventContextStorage implements EventContextStorage, EventContextListener, RedisConfigRegistryListener{
	private final Logger slog = LoggerFactory.getLogger(RedisEventContextStorage.class);

	@Requires
	private EventContextService eventContextService;

	@Requires
	private RedisConfigRegistry configReg;

	// topic to subscribers
	private ConcurrentHashMap<String, CopyOnWriteArraySet<EventSubscriber>> subscribers;

	private Pool<Jedis> jedisPool;

	private Jedis jedis; 

	private Object jedisLock = new Object();

	Map<HostAndPort, Jedis> aliveJedisServers = new ConcurrentHashMap<HostAndPort, Jedis> ();

	final static String keyPrefix = "evtcxtkey:";

	volatile static boolean subscribeStopRequested = false;

	@Override
	public String getName() {
		return "redis";
	}

	@Validate
	public void start(){
		try {
			if(!redisMode())
				return;
			
			configReg.addListener(this);
			subscribers = new ConcurrentHashMap<String, CopyOnWriteArraySet<EventSubscriber>>();
			subscribeStopRequested = false;
			jedisConnect();

			if(jedis != null)
				jedis.flushDB();

		} catch (Exception e) {
			slog.debug("araqne logdb cep: failed to start redis storage", e);
		}

		try {
			new Thread(new Runnable() {

				@Override
				public void run() {
					ExecutorService executor = Executors.newFixedThreadPool(10);

					while (!subscribeStopRequested) {
						for (HostAndPort server : getRegisterdServers()) {

							if (aliveJedisServers.keySet().contains(server))
								continue;

							Jedis jedisForSub = null;
							try {
								jedisForSub = new Jedis(server.getHost(), server.getPort());
								Psub subscribeRegister = new Psub(server);
								aliveJedisServers.put(server, jedisForSub);
								executor.submit(subscribeRegister);
							} catch (Exception e) {
								closeClient(jedisForSub);
								aliveJedisServers.remove(server);
							}
						}
					}
					executor.shutdownNow();
				}
			}).start();
		} catch (Exception e) {
			slog.debug("araqne logdb cep: failed to start redis expire monitoring service", e);
		}

		eventContextService.registerStorage(this);
	}
	
	@Invalidate
	public void stop(){
		if(!redisMode())
			return;
		
		subscribeStopRequested =  true;
		if(eventContextService != null)
			eventContextService.unregisterStorage(this);
		configReg.removeListener(this);

		for( Jedis aliveJedis: aliveJedisServers.values()){
			closeClient(aliveJedis);
		}

		aliveJedisServers.clear();

		try {
			returnResource(jedis);
			closeClient(jedis);

			if (jedisPool != null) {
				jedisPool.close();
				jedisPool.destroy();
				jedisPool = null;
			}
		} catch (Exception e) {
			slog.debug("araqne logdb cep: failed to close redis storage", e);
		}
	}

	private boolean redisMode(){
		String engine = System.getProperty("araqne.logdb.cepengine");
		if(engine == null || engine.isEmpty() || !engine.equals("redis"))
			return false;
		
		return true;
	}
	
	private void jedisConnect(){
		jedisPool = null;
		jedis = null;

		jedisPool = getPool();
		jedis = jedisPool.getResource();

		String password = configReg.getConfig().getPassword();
		if(password != null && !password.trim().isEmpty()){
			jedis.auth(password);
		}
	}

	private void connectCheck(){
		if(jedis == null){
			throw new JedisConnectionException("Redis server disconected [" + 
					configReg.getConfig().getHost() + ":" + configReg.getConfig().getPort()+"]") ;
		}

		//	if(	jedis.getClient().isBroken()){
		returnResource(jedis);
		closeClient(jedis);
		jedis = jedisPool.getResource();
		String password = configReg.getConfig().getPassword();
		if(password != null && !password.trim().isEmpty()){
			jedis.auth(password);
		}
		//}
	}

	private List<HostAndPort> getRegisterdServers(){
		List<HostAndPort> servers = new ArrayList<HostAndPort> ();
		RedisConfig config = configReg.getConfig();

		if(config.isSentinel()){
			Jedis sentinel = new Jedis(config.getHost(), config.getPort());
			try {
				List<Map<String, String>> slaves = sentinel.sentinelSlaves(config.getSentinelName());
				for(Map<String, String> slave : slaves){
					String host = slave.get("ip");
					int port = Integer.parseInt(slave.get("port"));
					servers.add(new HostAndPort(host, port));
				}
			} finally  {
				sentinel.close(); 
			}

			Set<String> sentinels = new HashSet<String>();
			sentinels.add(config.getHost() + ":" + config.getPort());
			servers.add(((JedisSentinelPool)jedisPool).getCurrentHostMaster());

		} else {
			servers.add(new HostAndPort(config.getHost(), config.getPort()));
		}

		return servers;
	}

	private Pool<Jedis> getPool(){
		RedisConfig config = configReg.getConfig();
		String host = config.getHost();
		int port = config.getPort();

		if(config.isSentinel()){
			Set<String> sentinels = new HashSet<String>();
			sentinels.add(host + ":" + port);
			return  new JedisSentinelPool(config.getSentinelName(), sentinels);
		}else {
			return new JedisPool(new JedisPoolConfig(), host, port, 10000);
		}
	}



	@Override
	public Set<String> getHosts(){
		return new HashSet<String>();
	}

	@Override
	public EventClock getClock(String host){
		return null;
	}

	@Override
	public Set<EventKey> getContextKeys(){
		return getContextKeys("*");
	}

	@Override
	public Set<EventKey> getContextKeys(String topic){
		HashSet<EventKey> keySet = new HashSet<EventKey>();
		Set<byte[]> keys = scan(topic);

		for(byte[] key : keys){
			try {
				keySet.add(parseKey(key));
			} catch (Exception e) {
			}
		}

		return keySet;
	}

	private Set<byte[]> scan(String topic){

		Set<byte[]> matchingKeys = new HashSet<byte[]> (); 
		ScanParams params = new ScanParams();
		String keyPrefix = topic + ":*";

		params.match(keyPrefix.getBytes());
		params.count(5000);

		byte[] cursor = ScanParams.SCAN_POINTER_START_BINARY;
		synchronized (jedisLock) {
			while(true){
				connectCheck();
				ScanResult<byte[]> result = jedis.scan(cursor, params);
				cursor = result.getCursorAsBytes();

				for( byte[] bkey : result.getResult())
					matchingKeys.add(bkey);

				if(Arrays.equals(result.getCursorAsBytes(), ScanParams.SCAN_POINTER_START_BINARY))
					break;
			}
		}
		return matchingKeys;
	}

	@SuppressWarnings("unchecked")
	@Override
	public  EventContext getContext(EventKey ctx){
		Response<byte []> value = null;
		synchronized (jedisLock) {
			Transaction t = jedis.multi();
			value = t.get(EventKey.marshal(ctx));
			t.exec();
		}
		byte[] byteValue = value.get();
		if(byteValue == null)
			return null;
		else
			return  EventContext.parse((Map<String, Object>) decode(byteValue));
	}

	private static String getDummyKey(byte[] key) {
		return keyPrefix + new String(Base64.encode(key));
	}

	@SuppressWarnings("unchecked")
	@Override
	public void removeContext(EventKey key, EventContext ctx, EventCause cause) {
		Response<byte[]> oldValue = null;
		synchronized (jedisLock) {
			connectCheck();
			Transaction t = jedis.multi();
			byte[] encodedKey = EventKey.marshal(key);

			oldValue = t.get(encodedKey);
			t.del(encodedKey);
			t.del(getDummyKey(encodedKey));
			//TODO :삭제 성공 할때  logtick 관련 작업 추가 
			t.exec();
		}

		byte[] response =oldValue.get();
		if(response !=null){ 
			EventContext oldCtx = EventContext.parse((Map<String, Object>) decode (response));
			generateEvent(EventContext.merge(oldCtx, ctx), cause);
		}
	}

	@Override
	public void advanceTime(String host, long now) {
		// TODO Auto-generated method stub
	}

	@Override
	public void clearClocks() {
		// TODO Auto-generated method stub
	}

	@Override
	public void clearContexts() {
		clearContexts("*");
	}

	@Override
	public void clearContexts(String topic) {
		Set<EventKey> keys = getContextKeys(topic);
		synchronized (jedisLock) {
			connectCheck();
			Transaction t = jedis.multi();

			for(EventKey key : keys){
				t.del(EventKey.marshal(key));
				t.del(getDummyKey(EventKey.marshal(key)));
			}

			t.exec();
		}
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
	@Deprecated 
	public void onUpdateTimeout(EventContext ctx) {
		//		Jedis jedis = (Jedis)jedisPool.getResource();
		//		byte[] key = encode(ctx.getKey());
		//		long timeoutTime = ctx.getTimeoutTime(); 
		//
		//		jedis.pexpireAt(getDummyKey(key), timeoutTime);
		//		returnResource(jedis);
	}

	@Override
	public void addContexts(Map<EventKey, EventContext> contexts) {
		ConcurrentHashMap<EventKey, Response<byte []> > responses = new ConcurrentHashMap<EventKey, Response<byte []>>();
		synchronized (jedisLock) {
			connectCheck();
			Transaction t = jedis.multi();
			for(EventKey evtkey : contexts.keySet()){
				byte[] key = EventKey.marshal(evtkey);
				Response<byte []> pipeString = t.get(key);
				responses.put(evtkey, pipeString);
			}
			t.exec();

			t = jedis.multi();
			for(EventKey evtkey : contexts.keySet()){
				byte[] response = responses.get(evtkey).get();
				byte[] key = EventKey.marshal(evtkey);
				String dummyKey = getDummyKey(key);
				EventContext ctx = contexts.get(evtkey);
				long timeoutTime = ctx.getTimeoutTime(); 

				if(response !=null){ 
					@SuppressWarnings("unchecked")
					EventContext oldCtx = 	EventContext.parse((Map<String, Object>) decode (response));

					if(timeoutTime != 0L)
						t.pexpireAt(dummyKey, timeoutTime);

					t.set(key, encode(EventContext.merge(oldCtx, ctx)));
				}else {
					t.set(dummyKey, "");
					t.set(key, encode(ctx));

					long expireTime = ctx.getExpireTime();
					long min = Math.min(timeoutTime, expireTime);
					long max = Math.max(timeoutTime, expireTime);
					if(min > 0)
						t.pexpireAt(dummyKey, min);
					else if(max > 0)
						t.pexpireAt(dummyKey, max);
				}
			}
			t.exec();
		}
	}

	@Override
	public EventContext addContext(EventContext ctx) {
		byte[] key = EventKey.marshal(ctx.getKey());
		String dummyKey = getDummyKey(key);

		synchronized (jedisLock) {
			connectCheck();
			byte[] oldByteValue = jedis.get(key);
			if(oldByteValue != null) {
				@SuppressWarnings("unchecked")
				EventContext oldCtx =  EventContext.parse((Map<String, Object>) decode(oldByteValue));
				ctx = oldCtx = EventContext.merge(oldCtx, ctx);
			} 

			byte[] value = encode(ctx);
			long timeoutTime = ctx.getTimeoutTime();
			long expireTime = ctx.getExpireTime();
			connectCheck();
			Transaction t= jedis.multi();
			t.set(dummyKey, "");
			t.set(key, value);

			long min = Math.min(timeoutTime, expireTime);
			long max = Math.max(timeoutTime, expireTime);
			if(min > 0L)
				t.pexpireAt(dummyKey, min);
			else if(max > 0L)
				t.pexpireAt(dummyKey, max);

			t.exec();
		}
		return ctx;
	}

	@Override
	public void removeContexts(Map<EventKey, EventContext> contexts, EventCause cause) {
		ConcurrentHashMap<EventKey, Response<byte []> > responses = new ConcurrentHashMap<EventKey, Response<byte []>>();
		synchronized (jedisLock) {
			connectCheck();
			Transaction t = jedis.multi();

			for(EventKey evtkey : contexts.keySet()){
				byte[] encodedKey = EventKey.marshal(evtkey);
				Response<byte []> oldValue = t.get(encodedKey);
				responses.put(evtkey, oldValue);
				t.del(encodedKey);
				t.del(getDummyKey(encodedKey));
			}

			t.exec();
		}

		for(EventKey evtkey : contexts.keySet()){
			byte[] response = responses.get(evtkey).get();
			if(response !=null){ 

				@SuppressWarnings("unchecked")
				EventContext oldCtx = 	EventContext.parse((Map<String, Object>) decode (response));

				generateEvent(EventContext.merge(oldCtx,  contexts.get(evtkey)), cause);
			}
		}
	}

	@Override
	public void onChanged() {
		jedisConnect();
	}

	private EventKey parseKey(byte[] key){
		return EventKey.parse(key);
	}

	private void returnResource(Jedis jedis){
		if (jedis != null) {
			try {
				jedisPool.returnResource(jedis);
			} catch (Exception e) {
			}
		}
	}

	public void closeClient(Jedis jedis){
		if(jedis != null)
			try{
				jedis.disconnect(); 
				jedis.close();
			}catch (Exception e) {
			}
	}

	public static byte[] encode(Marshalable obj){
		ByteBuffer bb = ByteBuffer.allocate(EncodingRule.lengthOf(obj.marshal()));
		EncodingRule.encode(bb, obj.marshal());
		return bb.array();
	}

	public static Object decode(byte[] bytes){
		return EncodingRule.decode(ByteBuffer.wrap(bytes));
	}

	private class  Psub implements Callable<Long> {
		HostAndPort server;

		private Psub(HostAndPort server){
			this.server = server;
		}

		@Override
		public Long call() throws Exception {
			RedisConfig config = configReg.getConfig();
			Jedis registeredJedis =  aliveJedisServers.get(server);

			try {
				String password = config.getPassword();
				if(password != null && !password.trim().isEmpty()){
					registeredJedis.auth(password);
				}

				/*redis event 수신 모드 on*/
				registeredJedis.configSet("notify-keyspace-events", "AKE");

				registeredJedis.psubscribe(new ExpireSub(), "__keyevent@0__:expired");
			} catch (Exception e) {
				closeClient(registeredJedis);
				aliveJedisServers.remove(server);
			}
			return 0L;
		}
	}

	private class ExpireSub extends JedisPubSub {
		@Override
		public void onPMessage(String pattern, String channel, String message){
			String keyPreFix = RedisEventContextStorage.keyPrefix;

			if(!message.startsWith(keyPreFix))
				return;
			EventKey key = null;

			try {
				byte[] bytes = Base64.decode(message.substring(keyPreFix.length()));
				key = parseKey(bytes);
				removeContext(key ,  new EventContext(key, 0L, 0L, 0L, 0, null), EventCause.EXPIRE);
			} catch (Exception e) {
				slog.debug("araqne logdb cep : event key parse error (" + key + ")", e);
			}		
		}
	}

}


