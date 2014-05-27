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
package org.araqne.logdb.mongo.script;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.araqne.api.Script;
import org.araqne.api.ScriptArgument;
import org.araqne.api.ScriptContext;
import org.araqne.api.ScriptUsage;
import org.araqne.logdb.mongo.MongoProfile;
import org.araqne.logdb.mongo.MongoProfileRegistry;

public class MongoScript implements Script {

	private MongoProfileRegistry profileRegistry;
	private ScriptContext context;

	public MongoScript(MongoProfileRegistry profileRegistry) {
		this.profileRegistry = profileRegistry;
	}

	@Override
	public void setScriptContext(ScriptContext context) {
		this.context = context;
	}

	public void profiles(String[] args) {
		context.println("MongoDB Profiles");
		context.println("-------------------");

		for (MongoProfile p : profileRegistry.getProfiles()) {
			context.println(p);
		}
	}

	// TODO: support credential config
	@ScriptUsage(description = "create mongodb profile", arguments = {
			@ScriptArgument(name = "profile name", type = "string", description = "profile name"),
			@ScriptArgument(name = "address", type = "string", description = "mongod host name or IP address"),
			@ScriptArgument(name = "port", type = "int", description = "mongod port, default 27017", optional = true),
			@ScriptArgument(name = "database", type = "string", description = "default database name", optional = true) })
	public void createProfile(String[] args) throws UnknownHostException {
		// verify host name
		InetAddress.getByName(args[1]);

		MongoProfile profile = new MongoProfile();
		profile.setName(args[0]);
		profile.setHost(args[1]);

		if (args.length > 2)
			profile.setPort(Integer.parseInt(args[2]));

		if (args.length > 3)
			profile.setDefaultDatabase(args[3]);

		profileRegistry.createProfile(profile);
		context.println("created");
	}

	@ScriptUsage(description = "remove mongodb profile", arguments = { @ScriptArgument(name = "profile name", type = "string", description = "profile name") })
	public void removeProfile(String[] args) {
		profileRegistry.removeProfile(args[0]);
		context.println("removed");
	}
}
