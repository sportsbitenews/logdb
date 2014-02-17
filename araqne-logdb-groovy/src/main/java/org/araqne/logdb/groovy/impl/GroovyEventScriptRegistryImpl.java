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
package org.araqne.logdb.groovy.impl;

import groovy.util.GroovyScriptEngine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
import org.araqne.logdb.groovy.GroovyEventScript;
import org.araqne.logdb.groovy.GroovyEventScriptRegistry;
import org.araqne.logdb.groovy.GroovyEventSubscription;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name = "logdb-groovy-event-script-registry")
@Provides(specifications = { GroovyEventScriptRegistry.class })
public class GroovyEventScriptRegistryImpl implements GroovyEventScriptRegistry, EventSubscriber {
	private final Logger slog = LoggerFactory.getLogger(GroovyEventScriptRegistryImpl.class);

	@Requires
	private ConfigService conf;

	@Requires
	private EventContextService eventContextService;

	private BundleContext bc;
	private GroovyScriptEngine gse;

	private Set<GroovyEventSubscription> subscriptions;

	// script name to script mappings
	private ConcurrentHashMap<String, GroovyEventScript> scripts;

	// topic to script mappings
	private ConcurrentHashMap<String, List<GroovyEventScript>> topicScripts;

	public GroovyEventScriptRegistryImpl(BundleContext bc) {
		this.bc = bc;
	}

	@Validate
	public void start() throws IOException {
		subscriptions = Collections.synchronizedSet(new HashSet<GroovyEventSubscription>());
		scripts = new ConcurrentHashMap<String, GroovyEventScript>();
		topicScripts = new ConcurrentHashMap<String, List<GroovyEventScript>>();

		String path = ScriptPaths.getPath("event_scripts");
		gse = new GroovyScriptEngine(path);

		loadScripts();

		eventContextService.addSubscriber("*", this);
	}

	private void loadScripts() {
		ConfigDatabase db = conf.ensureDatabase("araqne-logdb-groovy");

		ConfigIterator it = db.findAll(GroovyEventSubscription.class);
		for (GroovyEventSubscription s : it.getDocuments(GroovyEventSubscription.class)) {
			loadScript(s);
		}
	}

	private void loadScript(GroovyEventSubscription s) {
		List<GroovyEventScript> handlers = Collections.synchronizedList(new ArrayList<GroovyEventScript>());
		List<GroovyEventScript> oldHandlers = topicScripts.putIfAbsent(s.getTopic(), handlers);
		if (oldHandlers != null)
			handlers = oldHandlers;

		GroovyEventScript script = newScript(s.getScriptName());
		handlers.add(script);

		scripts.put(s.getScriptName(), script);
		subscriptions.add(s);
		slog.info("araqne logdb groovy: subscribe topic [{}], event script [{}]", s.getTopic(), s.getScriptName());
	}

	private void unloadScript(GroovyEventSubscription s) {
		List<GroovyEventScript> handlers = topicScripts.get(s.getTopic());
		if (handlers == null)
			return;

		GroovyEventScript script = scripts.remove(s.getScriptName());
		if (script == null)
			return;

		handlers.remove(script);
	}

	@Invalidate
	public void stop() {
		eventContextService.removeSubscriber("*", this);

		if (gse != null)
			gse.getGroovyClassLoader().clearCache();

		scripts.clear();
		topicScripts.clear();
	}

	@Override
	public void onEvent(Event event) {
		EventKey key = event.getKey();
		List<GroovyEventScript> scripts = topicScripts.get(key.getTopic());
		if (scripts == null) {
			if (slog.isDebugEnabled())
				slog.debug("araqne logdb groovy: no matching event script, topic [{}] key [{}]", key.getTopic(), key.getKey());

			return;
		}

		for (GroovyEventScript script : scripts) {
			try {
				script.onEvent(event);
			} catch (Throwable t) {
				slog.warn("araqne logdb groovy: event script should not throw any exception", t);
			}
		}
	}

	@Override
	public List<GroovyEventSubscription> getEventSubscriptions(String topicFilter) {
		ArrayList<GroovyEventSubscription> l = new ArrayList<GroovyEventSubscription>();
		for (GroovyEventSubscription s : subscriptions) {
			if (topicFilter != null && !topicFilter.equals(s.getTopic()))
				continue;

			l.add(s);
		}

		return l;
	}

	@Override
	public void subscribeEvent(GroovyEventSubscription subscription) {
		if (!subscriptions.add(subscription))
			throw new IllegalStateException("duplicated event subscription, topic [" + subscription.getTopic() + "], script ["
					+ subscription.getScriptName() + "]");

		loadScript(subscription);

		ConfigDatabase db = conf.ensureDatabase("araqne-logdb-groovy");
		db.add(subscription, "araqne-logdb-groovy", "subscribe event");
	}

	@Override
	public void unsubscribeEvent(GroovyEventSubscription subscription) {
		if (!subscriptions.remove(subscription))
			throw new IllegalStateException("event subscription not found, topic [" + subscription.getTopic() + "], script ["
					+ subscription.getScriptName() + "]");

		unloadScript(subscription);

		Map<String, Object> pred = new HashMap<String, Object>();
		pred.put("topic", subscription.getTopic());
		pred.put("scriptName", subscription.getScriptName());

		ConfigDatabase db = conf.ensureDatabase("araqne-logdb-groovy");
		Config c = db.findOne(GroovyEventSubscription.class, Predicates.field(pred));
		if (c != null)
			db.remove(c, false, "araqne-logdb-groovy", "subscribe event");
	}

	@Override
	public void reloadScript(String scriptName) {
		for (GroovyEventSubscription s : getEventSubscriptions(null)) {
			if (s.getScriptName().equals(scriptName)) {
				unloadScript(s);
				loadScript(s);
			}
		}
	}

	@Override
	public GroovyEventScript newScript(String scriptName) {
		try {
			Class<?> clazz = gse.loadScriptByName(scriptName + ".groovy");
			Object o = clazz.newInstance();
			GroovyEventScript script = (GroovyEventScript) o;
			script.setBundleContext(bc);
			return script;
		} catch (Throwable t) {
			throw new IllegalStateException("cannot instanciate groovy event script: " + scriptName, t);
		}
	}
}
