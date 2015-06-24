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
package org.araqne.logdb.cep.redis;

import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.confdb.Config;
import org.araqne.confdb.ConfigDatabase;
import org.araqne.confdb.ConfigService;
import org.araqne.confdb.Predicates;

import redis.clients.jedis.exceptions.JedisConnectionException;

@Component(name = "redis-config-reg")
@Provides
public class RedisConfigRegistryImpl  implements RedisConfigRegistry{

	@Requires
	private ConfigService conf;

	private RedisConfig config;
	
	private CopyOnWriteArraySet<RedisConfigRegistryListener> listeners;

	@Validate
	public void start(){
		ConfigDatabase db = conf.ensureDatabase("logpresso-cep");
		//ConfigIterator it = db.findAll(RedisConfig.class);
		Config c= db.findOne(RedisConfig.class,  Predicates.field("name", "rediscep"));
		if(c !=null)
			config = c.getDocument(RedisConfig.class);
		
		listeners = new CopyOnWriteArraySet<RedisConfigRegistryListener>();
	}
	
	@Invalidate
	public void stop(){
		listeners.clear();
	}
	
	@Override
	public  void createConfig(RedisConfig config) throws JedisConnectionException{
		if (config == null)
			throw new IllegalArgumentException("ssh profile can not be null");

		this.config = config;
		
		ConfigDatabase db = conf.ensureDatabase("logpresso-cep");
		Config c= db.findOne(RedisConfig.class,  Predicates.field("name", "rediscep"));
		
		if(c == null)
			db.add(config);
		else 
			db.update(c, config);

		for (RedisConfigRegistryListener listener : listeners) {
				listener.onChanged();
		}
	}
	
	@Override 
	public RedisConfig getConfig(){
		return config;
	}

	@Override
	public void addListener(RedisConfigRegistryListener listener) {
		listeners.add(listener);
	}

	@Override
	public void removeListener(RedisConfigRegistryListener listener) {
		listeners.remove(listener);
	}
	
}
