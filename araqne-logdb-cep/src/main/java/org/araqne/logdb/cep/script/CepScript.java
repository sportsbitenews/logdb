/*
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
package org.araqne.logdb.cep.script;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.araqne.api.Script;
import org.araqne.api.ScriptArgument;
import org.araqne.api.ScriptContext;
import org.araqne.api.ScriptUsage;
import org.araqne.logdb.cep.EventClock;
import org.araqne.logdb.cep.EventClockItem;
import org.araqne.logdb.cep.EventContextService;
import org.araqne.logdb.cep.EventContextStorage;
import org.araqne.logdb.cep.EventKey;

public class CepScript implements Script {
	private ScriptContext context;

	private EventContextService eventContextService;

	public CepScript(EventContextService eventContextService) {
		this.eventContextService = eventContextService;
	}

	@Override
	public void setScriptContext(ScriptContext context) {
		this.context = context;
	}

	@ScriptUsage(description = "set clock time manually", arguments = {
			@ScriptArgument(name = "host", type = "string", description = "clock host"),
			@ScriptArgument(name = "time", type = "string", description = "yyyyMMddHHmmss format") })
	public void setClock(String[] args) {
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
		String host = args[0];
		Date d = df.parse(args[1], new ParsePosition(0));
		if (d == null) {
			context.println("invalid time, follow yyyyMMddHHmmss format");
			return;
		}

		EventContextStorage storage = getEventStorage();
		EventClock<? extends EventClockItem> clock = storage.getClock(host);
		if (clock == null) {
			context.println("clock not found");
			return;
		}

		clock.setTime(d.getTime(), true);
		context.println("set");
	}

	public void clearContexts(String[] args) throws InterruptedException {
		context.print("All event contexts will be Deleted. continue? (Y/n)");
		String s = context.readLine();
		if (s.equalsIgnoreCase("n")) {
			return;
		}
		
		EventContextStorage storage = getEventStorage();
		storage.clearContexts();
		context.println("completed");
	}

	public void clearClocks(String[] args) throws InterruptedException {
		context.print("All event contexts will be Deleted. continue? (Y/n)");
		String s = context.readLine();
		if (s.equalsIgnoreCase("n")) {
			return;
		}

		EventContextStorage storage = getEventStorage();
		storage.clearClocks();
		context.println("completed");
	}

	@ScriptUsage(description = "print external clocks", arguments = {
			@ScriptArgument(name = "host", type = "string", description = "clock host"),
			@ScriptArgument(name = "queue type", type = "string", description = "expire or timeout") })
	public void clockQueue(String[] args) {
		String host = args[0];
		String queueType = args[1];

		EventContextStorage storage = getEventStorage();
		EventClock<? extends EventClockItem> clock = storage.getClock(host);
		if (clock == null) {
			context.println("clock not found");
			return;
		}

		if (!queueType.equals("timeout") && !queueType.equals("expire")) {
			context.println("invalid queue type");
			return;
		}

		context.println("Event Clock Queue (" + queueType + ")");
		context.println("---------------------------");

		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		if (queueType.equals("timeout")) {
			for (EventClockItem item : clock.getTimeoutContexts()) {
				context.println("timeout [" + df.format(item.getTimeoutTime()) + "] " + item.getKey());
			}

		} else if (queueType.equals("expire")) {
			for (EventClockItem item : clock.getExpireContexts()) {
				context.println("expire [" + df.format(item.getExpireTime()) + "] " + item.getKey());
			}
		}
	}

	public void engine(String[] args) {
		EventContextStorage storage = getEventStorage();
		context.println("cep engine : " + storage.getName());
	}

	@ScriptUsage(description = "print external clocks", arguments = {
			@ScriptArgument(name = "offset", type = "int", description = "offset", optional = true),
			@ScriptArgument(name = "limit", type = "int", description = "limit", optional = true) })
	public void clocks(String[] args) {
		int offset = 0;
		int limit = 100;

		if (args.length > 0)
			offset = Integer.parseInt(args[0]);

		if (args.length > 1)
			limit = Integer.parseInt(args[1]);

		context.println("Event Clocks");
		context.println("-----------------");

		EventContextStorage storage = getEventStorage();
		List<String> hosts = new ArrayList<String>(storage.getHosts());
		List<String> page = hosts.subList(Math.min(offset, hosts.size()), Math.min(offset + limit, hosts.size()));

		for (String host : page) {
			EventClock<? extends EventClockItem> clock = storage.getClock(host);
			context.println(clock);
		}

		context.println("total " + hosts.size() + " clocks");
	}

	@ScriptUsage(description = "print contexts", arguments = {
			@ScriptArgument(name = "offset", type = "int", description = "offset", optional = true),
			@ScriptArgument(name = "limit", type = "int", description = "limit", optional = true) })
	public void contexts(String[] args) {
		int offset = 0;
		int limit = 100;

		if (args.length > 0)
			offset = Integer.parseInt(args[0]);

		if (args.length > 1)
			limit = Integer.parseInt(args[1]);

		context.println("Event Contexts");
		context.println("-----------------");

		EventContextStorage storage = getEventStorage();
		Iterator<EventKey> itr = storage.getContextKeys();
		int size = 0;
		while (itr.hasNext()) {
			EventKey key = itr.next();

			if (size < offset) {
				size++;
				continue;
			}

			if (limit + offset > size)
				context.println(key);

			size++;
		}

		context.println("total " + size + " contexts");
	}

	private EventContextStorage getEventStorage() {
		return eventContextService.getDefaultStorage();
	}
}
