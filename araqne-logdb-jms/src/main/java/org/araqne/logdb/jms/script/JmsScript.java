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
package org.araqne.logdb.jms.script;

import org.araqne.api.Script;
import org.araqne.api.ScriptArgument;
import org.araqne.api.ScriptContext;
import org.araqne.api.ScriptUsage;
import org.araqne.logdb.jms.JmsProfile;
import org.araqne.logdb.jms.JmsProfileRegistry;

public class JmsScript implements Script {

	private ScriptContext context;
	private JmsProfileRegistry profileRegistry;

	public JmsScript(JmsProfileRegistry profileRegistry) {
		this.profileRegistry = profileRegistry;
	}

	@Override
	public void setScriptContext(ScriptContext context) {
		this.context = context;
	}

	public void profiles(String[] args) {
		context.println("JMS Profiles");
		context.println("--------------");

		for (JmsProfile p : profileRegistry.getProfiles()) {
			context.println(p.toString());
		}
	}

	@ScriptUsage(description = "create jms profile", arguments = {
			@ScriptArgument(name = "profile name", type = "string", description = "jms profile name"),
			@ScriptArgument(name = "endpoint", type = "string", description = "jms endpoint, e.g. tcp://hostname:port"),
			@ScriptArgument(name = "queue name", type = "string", description = "jms queue name") })
	public void createProfile(String[] args) {
		JmsProfile profile = new JmsProfile();
		profile.setName(args[0]);
		profile.setEndpoint(args[1]);
		profile.setQueueName(args[2]);

		profileRegistry.createProfile(profile);
		context.println("created");
	}

	@ScriptUsage(description = "remove jms profile", arguments = { @ScriptArgument(name = "profile name", type = "string", description = "profile name") })
	public void removeProfile(String[] args) {
		profileRegistry.removeProfile(args[0]);
		context.println("removed");
	}
}
