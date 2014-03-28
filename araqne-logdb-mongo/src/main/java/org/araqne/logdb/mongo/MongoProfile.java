/**
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
package org.araqne.logdb.mongo;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.araqne.api.CollectionTypeHint;
import org.araqne.api.FieldOption;
import org.araqne.confdb.CollectionName;

import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

@CollectionName("mongo_profiles")
public class MongoProfile {
	@FieldOption(nullable = false)
	private String name;

	@FieldOption(nullable = false)
	private String host;

	@FieldOption(nullable = false)
	private int port = 27017;

	@FieldOption(nullable = true)
	private String defaultDatabase;

	@CollectionTypeHint(MongoAuthProfile.class)
	private List<MongoAuthProfile> authProfiles;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
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

	public String getDefaultDatabase() {
		return defaultDatabase;
	}

	public void setDefaultDatabase(String defaultDatabase) {
		this.defaultDatabase = defaultDatabase;
	}

	public List<MongoAuthProfile> getAuthProfiles() {
		return authProfiles;
	}

	public void setAuthProfiles(List<MongoAuthProfile> authProfiles) {
		this.authProfiles = authProfiles;
	}

	public ServerAddress getAddress() throws UnknownHostException {
		return new ServerAddress(host, port);
	}

	public List<MongoCredential> getCredentials() {
		if (authProfiles == null || authProfiles.isEmpty())
			return null;

		ArrayList<MongoCredential> credentials = new ArrayList<MongoCredential>();

		for (MongoAuthProfile p : authProfiles) {
			MongoCredential c = MongoCredential.createMongoCRCredential(p.getUser(), p.getDb(), p.getPassword().toCharArray());
			credentials.add(c);
		}

		return credentials;
	}

	@Override
	public String toString() {
		return "[" + name + "] " + host + ":" + port + ", db=" + defaultDatabase + ", credentials=" + authProfiles;
	}

}
