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

import org.araqne.confdb.CollectionName;

@CollectionName("redis_cep_profiles")
public class RedisConfig {
	public String name = "rediscep";
	private String host = "127.0.0.1";
	private int port = 6379;
	private boolean isSentinel = false;
	private String password = null;
	private String sentinelName = "mymaster";

	public RedisConfig() {
	}

	public RedisConfig(String host, int port) {
		this.host = host;
		this.port = port;
	}

	public String getName() {
		return name;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public boolean isSentinel() {
		return isSentinel;
	}

	public void setSentinel(boolean isSentinel) {
		this.isSentinel = isSentinel;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getSentinelName() {
		return sentinelName;
	}

	public void setSentinelName(String sentinelName) {
		this.sentinelName = sentinelName;
	}

	@Override
	public String toString() {
		if(isSentinel)
			return "host=" + host + ", port=" + ", isSentinel=" + isSentinel;
		
		return "host=" + host + ", port=" + ", is_sentinel=" + isSentinel + ", sentinel_name=" + sentinelName;
	}
}
