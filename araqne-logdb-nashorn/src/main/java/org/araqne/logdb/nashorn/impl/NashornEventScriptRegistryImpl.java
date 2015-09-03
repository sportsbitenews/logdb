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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.confdb.Config;
import org.araqne.confdb.ConfigDatabase;
import org.araqne.confdb.ConfigIterator;
import org.araqne.confdb.ConfigService;
import org.araqne.confdb.Predicates;
import org.araqne.logdb.cep.Event;
import org.araqne.logdb.cep.EventContextService;
import org.araqne.logdb.cep.EventKey;
import org.araqne.logdb.cep.EventSubscriber;
import org.araqne.logdb.nashorn.NashornEventScript;
import org.araqne.logdb.nashorn.NashornEventScriptRegistry;
import org.araqne.logdb.nashorn.NashornEventSubscription;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name = "logdb-nashorn-event-script-registry")
@Provides(specifications = { NashornEventScriptRegistry.class })
public class NashornEventScriptRegistryImpl implements NashornEventScriptRegistry, EventSubscriber {
	private final Logger slog = LoggerFactory.getLogger(NashornEventScriptRegistryImpl.class);

	@Requires
	private ConfigService conf;

	@Requires
	private EventContextService eventContextService;

	private BundleContext bc;

	private File scriptDir;

	private ScriptEngineManager factory;

	private Set<NashornEventSubscription> subscriptions;

	// script name to script mappings
	private ConcurrentHashMap<String, NashornEventScript> scripts;

	// topic to script name mappings. use script name instead of script itself
	// to support one-script-to-many-topics and reload script scenario.
	private ConcurrentHashMap<String, Set<String>> topicScripts;

	public NashornEventScriptRegistryImpl(BundleContext bc) {
		this.bc = bc;
		this.factory = new ScriptEngineManager();
		this.scriptDir = ScriptPaths.getPath("event_scripts");
	}

	@Validate
	public void start() throws IOException {
		subscriptions = Collections.synchronizedSet(new HashSet<NashornEventSubscription>());
		scripts = new ConcurrentHashMap<String, NashornEventScript>();
		topicScripts = new ConcurrentHashMap<String, Set<String>>();

		loadScripts();

		eventContextService.addSubscriber("*", this);
	}

	private void loadScripts() {
		ConfigDatabase db = conf.ensureDatabase("araqne-logdb-nashorn");

		ConfigIterator it = db.findAll(NashornEventSubscription.class);
		for (NashornEventSubscription s : it.getDocuments(NashornEventSubscription.class)) {
			try {
				if (scripts.get(s.getScriptName()) == null) {
					NashornEventScript script = newScript(s.getScriptName());
					scripts.put(s.getScriptName(), script);
				}

				subscribe(s);
			} catch (Throwable t) {
				if (slog.isDebugEnabled()) {
					File f = new File(scriptDir, s.getScriptName() + ".js");
					slog.debug("araqne logdb nashorn: javascript event script [" + f.getAbsolutePath() + "] not found", t);
				}
			}
		}
	}

	private void subscribe(NashornEventSubscription s) {
		if (!subscriptions.add(s))
			throw new IllegalStateException("duplicated event subscription, topic [" + s.getTopic() + "], script ["
					+ s.getScriptName() + "]");

		Set<String> handlers = Collections.synchronizedSet(new HashSet<String>());
		Set<String> oldHandlers = topicScripts.putIfAbsent(s.getTopic(), handlers);
		if (oldHandlers != null)
			handlers = oldHandlers;

		handlers.add(s.getScriptName());

		slog.info("araqne logdb nashorn: subscribe topic [{}], event script [{}]", s.getTopic(), s.getScriptName());
	}

	private void unsubscribe(NashornEventSubscription s) {
		if (!subscriptions.remove(s))
			throw new IllegalStateException("event subscription not found, topic [" + s.getTopic() + "], script ["
					+ s.getScriptName() + "]");

		Set<String> handlers = topicScripts.get(s.getTopic());
		if (handlers == null)
			return;

		handlers.remove(s.getScriptName());
	}

	@Invalidate
	public void stop() {
		eventContextService.removeSubscriber("*", this);

		scripts.clear();
		topicScripts.clear();
	}

	@Override
	public void onEvent(Event event) {
		EventKey key = event.getKey();
		Set<String> scriptNames = topicScripts.get(key.getTopic());
		if (scriptNames == null) {
			if (slog.isDebugEnabled())
				slog.debug("araqne logdb nashorn: no matching event script, topic [{}] key [{}]", key.getTopic(), key.getKey());

			return;
		}

		for (String scriptName : scriptNames) {
			try {
				NashornEventScript script = scripts.get(scriptName);
				if (script == null)
					continue;

				script.onEvent(script, event);
			} catch (Throwable t) {
				slog.warn("araqne logdb nashorn: event script should not throw any exception", t);
			}
		}
	}

	@Override
	public List<NashornEventSubscription> getEventSubscriptions(String topicFilter) {
		ArrayList<NashornEventSubscription> l = new ArrayList<NashornEventSubscription>();
		for (NashornEventSubscription s : subscriptions) {
			if (topicFilter != null && !topicFilter.equals(s.getTopic()))
				continue;

			l.add(s);
		}

		return l;
	}

	@Override
	public void subscribeEvent(NashornEventSubscription subscription) {
		subscribe(subscription);

		ConfigDatabase db = conf.ensureDatabase("araqne-logdb-nashorn");
		db.add(subscription, "araqne-logdb-nashorn", "subscribe event");
	}

	@Override
	public void unsubscribeEvent(NashornEventSubscription subscription) {
		unsubscribe(subscription);

		Map<String, Object> pred = new HashMap<String, Object>();
		pred.put("topic", subscription.getTopic());
		pred.put("scriptName", subscription.getScriptName());

		ConfigDatabase db = conf.ensureDatabase("araqne-logdb-nashorn");
		Config c = db.findOne(NashornEventSubscription.class, Predicates.field(pred));
		if (c != null)
			db.remove(c, false, "araqne-logdb-nashorn", "subscribe event");
	}

	@Override
	public void reloadScript(String scriptName) {
		scripts.put(scriptName, newScript(scriptName));
	}

	@Override
	public NashornEventScript newScript(String scriptName) {
		ClassLoader oldLoader = Thread.currentThread().getContextClassLoader();
		try {
			Thread.currentThread().setContextClassLoader(NashornEventScript.class.getClassLoader());
			ScriptEngine nashornEngine = factory.getEngineByName("nashorn");
			if (nashornEngine == null)
				throw new IllegalStateException("cannot load nashorn engine for javascript " + scriptName);

			nashornEngine.eval("var EventScript = Java.type(\"org.araqne.logdb.nashorn.NashornEventScript\");");
			nashornEngine.eval(new FileReader(new File(scriptDir, scriptName + ".js")));
			NashornEventScript script = (NashornEventScript) nashornEngine.eval("new " + scriptName + "();");
			script.setBundleContext(bc);
			return script;
		} catch (Throwable t) {
			throw new IllegalStateException("cannot instanciate event javascript: " + scriptName, t);
		} finally {
			Thread.currentThread().setContextClassLoader(oldLoader);
		}
	}
}
