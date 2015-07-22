/**
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
package org.araqne.logdb.nashorn.impl;

import org.araqne.api.Script;
import org.araqne.api.ScriptArgument;
import org.araqne.api.ScriptContext;
import org.araqne.api.ScriptUsage;
import org.araqne.logdb.nashorn.NashornEventScriptRegistry;
import org.araqne.logdb.nashorn.NashornEventSubscription;

public class NashornScript implements Script {
	private ScriptContext context;
	private NashornEventScriptRegistry eventScriptRegistry;

	public NashornScript(NashornEventScriptRegistry eventScriptRegistry) {
		this.eventScriptRegistry = eventScriptRegistry;
	}

	@Override
	public void setScriptContext(ScriptContext context) {
		this.context = context;
	}

	@ScriptUsage(description = "list all event subscriptions", arguments = { @ScriptArgument(name = "topic filter", type = "string", description = "topic filter", optional = true) })
	public void eventSubscriptions(String[] args) {
		String topicFilter = null;
		if (args.length > 0)
			topicFilter = args[0];

		context.println("Javascript Event Subscriptions");
		context.println("--------------------------------");
		for (NashornEventSubscription s : eventScriptRegistry.getEventSubscriptions(topicFilter)) {
			context.println(s);
		}
	}

	@ScriptUsage(description = "subscribe event", arguments = {
			@ScriptArgument(name = "topic", type = "string", description = "topic name"),
			@ScriptArgument(name = "script name", type = "string", description = "script file name without .js extension") })
	public void subscribeEvent(String[] args) {
		eventScriptRegistry.reloadScript(args[1]);
		NashornEventSubscription s = new NashornEventSubscription();
		s.setTopic(args[0]);
		s.setScriptName(args[1]);
		eventScriptRegistry.subscribeEvent(s);
		context.println("subscribed");
	}

	@ScriptUsage(description = "unsubscribe event", arguments = {
			@ScriptArgument(name = "topic", type = "string", description = "topic name"),
			@ScriptArgument(name = "script name", type = "string", description = "script file name without .js extension") })
	public void unsubscribeEvent(String[] args) {
		NashornEventSubscription s = new NashornEventSubscription();
		s.setTopic(args[0]);
		s.setScriptName(args[1]);
		eventScriptRegistry.unsubscribeEvent(s);
		context.println("unsubscribed");
	}

	@ScriptUsage(description = "reload event script", arguments = { @ScriptArgument(name = "script name", type = "string", description = "script name") })
	public void reloadEventScript(String[] args) {
		try {
			// test availability
			String scriptName = args[0];
			eventScriptRegistry.newScript(scriptName);

			// reload
			eventScriptRegistry.reloadScript(scriptName);
			context.println("reloaded");
		} catch (IllegalStateException e) {
			context.println(e.getCause().getMessage());
		}
	}
}
