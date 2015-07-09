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
package org.araqne.logdb.cep.script;

import org.araqne.api.Script;
import org.araqne.api.ScriptContext;
import org.araqne.api.ScriptUsage;
import org.araqne.logdb.cep.redis.RedisConfig;
import org.araqne.logdb.cep.redis.RedisConfigRegistry;

import redis.clients.jedis.exceptions.JedisConnectionException;

public class RedisCepScript implements Script {
	private ScriptContext context;

	private RedisConfigRegistry redisConfigRegisty;

	public RedisCepScript(RedisConfigRegistry configRegistry) {
		this.redisConfigRegisty = configRegistry;
	}

	@Override
	public void setScriptContext(ScriptContext context) {
		this.context = context;
	}

	public void getConfig(String[] args) {
		context.println("Redis Configuration");
		context.println("----------------");

		RedisConfig config = redisConfigRegisty.getConfig();
		if(config == null)
			return;

		context.println("host : " + config.getHost());
		context.println("port : " + config.getPort());
		context.println("sentinel mode : " + config.isSentinel());
		context.println("sentinel name : " + config.getSentinelName());
		//context.println("password : " + config.getPassword());
	}

	@ScriptUsage(description = "restore defaults setting")
	public void restoreDefaults(String[] args) throws InterruptedException{
		context.print("Current settings will be Deleted. (y/n)");
		String s = context.readLine("n");
		if(s.equalsIgnoreCase("y")) {

			try {
				redisConfigRegisty.createConfig(new RedisConfig());
			} catch (JedisConnectionException e) {
				throw new IllegalStateException("cannot connect to jedis server [ " + new RedisConfig().getHost() + ":"  + new RedisConfig().getPort() +" ]");
			}finally{
				context.println("restored");
			}

		}else{
			context.println("canceled");
		}
	}

	@ScriptUsage(description = "setting redis configuration")
	public void setConfig(String[] args) throws InterruptedException  {
		RedisConfig redisConfig = redisConfigRegisty.getConfig();
		if(redisConfig == null) {
			redisConfig = new RedisConfig("127.0.0.1", 6379);
		}

		context.print("host? ");
		redisConfig.setHost(context.readLine(redisConfig.getHost()).trim());

		context.print("ip? ");
		String port = context.readLine(Integer.toString(redisConfig.getPort())).trim();
		redisConfig.setPort(Integer.parseInt(port));

		context.print("sentinel mode? ");
		String s = context.readLine(Boolean.toString(redisConfig.isSentinel())).trim();
		Boolean sentinelMode = Boolean.parseBoolean(s);
		redisConfig.setSentinel(sentinelMode);

		if(sentinelMode) {
			context.print("sentinel name? ");
			redisConfig.setSentinelName(context.readLine(redisConfig.getSentinelName()).trim());
		}

		context.print("password? ");
		String enteredPassword1 = context.readPassword();

		context.print("confirm password? ");
		String enteredPassword2 = context.readPassword();

		if (enteredPassword1.equals(enteredPassword2)) {
			if (!enteredPassword1.isEmpty())
				redisConfig.setPassword(enteredPassword1);
		} else
			throw new IllegalStateException("password does not match");

		try {
			redisConfigRegisty.createConfig(redisConfig);
		} catch (JedisConnectionException e) {
			throw new IllegalStateException("Redis server disconnected [" + redisConfig.getHost() + ":"  + redisConfig.getPort() +" ]");
		}finally{
			context.println("set");
		}
	}
}
