/*
 * Copyright 2012 Future Systems
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
package org.araqne.logdb.jython.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.api.PrimitiveConverter;
import org.araqne.confdb.Config;
import org.araqne.confdb.ConfigCollection;
import org.araqne.confdb.ConfigDatabase;
import org.araqne.confdb.ConfigIterator;
import org.araqne.confdb.ConfigService;
import org.araqne.confdb.Predicate;
import org.araqne.confdb.Predicates;
import org.araqne.jython.JythonService;
import org.araqne.logdb.LogQueryScript;
import org.araqne.logdb.LogQueryScriptFactory;
import org.araqne.logdb.LogQueryScriptRegistry;
import org.araqne.logdb.jython.JythonQueryScriptRegistry;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name = "jython-query-script-registry")
@Provides
public class JythonQueryScriptRegistryImpl implements JythonQueryScriptRegistry {
	private final Logger logger = LoggerFactory.getLogger(JythonQueryScriptRegistryImpl.class);

	@Requires
	private ConfigService conf;

	@Requires
	private JythonService jython;

	@Requires
	private LogQueryScriptRegistry logScriptRegistry;

	private ConcurrentMap<String, ConcurrentMap<String, PyObject>> workspaceScripts;

	@Validate
	public void start() {
		workspaceScripts = new ConcurrentHashMap<String, ConcurrentMap<String, PyObject>>();

		// load scripts
		ConfigDatabase db = conf.ensureDatabase("araqne-logdb-jython");
		ConfigCollection col = db.ensureCollection("scripts");
		ConfigIterator it = col.findAll();
		try {
			for (LogScriptConfig sc : it.getDocuments(LogScriptConfig.class)) {
				try {
					PyObject o = eval(sc.name, sc.script);

					ConcurrentMap<String, PyObject> scripts = new ConcurrentHashMap<String, PyObject>();
					ConcurrentMap<String, PyObject> old = workspaceScripts.putIfAbsent(sc.workspace, scripts);
					if (old != null)
						scripts = old;

					scripts.put(sc.name, o);
				} catch (Throwable t) {
					logger.error("araqne logdb jython: cannot load script [" + sc.name + "]", t);
				}
			}
		} finally {
			it.close();
		}

		// register all
		for (String workspace : workspaceScripts.keySet()) {
			ConcurrentMap<String, PyObject> scripts = workspaceScripts.get(workspace);
			for (String name : scripts.keySet())
				try {
					PyObject factory = scripts.get(name);
					logScriptRegistry.addScriptFactory(workspace, name, new LogScriptFactoryImpl(factory));
				} catch (Throwable t) {
					logger.error("araqne logdb jython: cannot register script [" + name + "]", t);
				}
		}
	}

	@Invalidate
	public void stop() {
		if (logScriptRegistry != null) {
			for (String workspace : workspaceScripts.keySet()) {
				ConcurrentMap<String, PyObject> scripts = workspaceScripts.get(workspace);
				for (String name : scripts.keySet())
					logScriptRegistry.removeScriptFactory(workspace, name);
			}
		}

		if (jython != null)
			jython.unregisterInterpreter("logdb");
	}

	@Override
	public Set<String> getWorkspaceNames() {
		return Collections.unmodifiableSet(workspaceScripts.keySet());
	}

	@Override
	public void dropWorkspace(String name) {

	}

	@Override
	public Set<String> getScriptNames(String workspace) {
		ConcurrentMap<String, PyObject> scripts = workspaceScripts.get(workspace);
		if (scripts == null)
			return null;
		return scripts.keySet();
	}

	@Override
	public String getScriptCode(String workspace, String name) {
		ConfigDatabase db = conf.ensureDatabase("araqne-logdb-jython");
		ConfigCollection col = db.ensureCollection("scripts");

		Config c = col.findOne(getPredicate(workspace, name));
		if (c == null)
			return null;

		LogScriptConfig sc = c.getDocument(LogScriptConfig.class);
		return sc.script;
	}

	@Override
	public LogQueryScript newLogScript(String workspace, String name, Map<String, Object> params) {
		ConcurrentMap<String, PyObject> scripts = workspaceScripts.get(workspace);
		if (scripts == null)
			return null;

		PyObject factory = scripts.get(name);
		if (factory == null)
			return null;

		PyObject instance = factory.__call__();
		return (LogQueryScript) instance.__tojava__(LogQueryScript.class);
	}

	@Override
	public void setScript(String workspace, String name, String script) {
		PyObject factory = eval(name, script);

		ConcurrentMap<String, PyObject> scripts = new ConcurrentHashMap<String, PyObject>();
		ConcurrentMap<String, PyObject> old = workspaceScripts.putIfAbsent(workspace, scripts);
		if (old != null)
			scripts = old;

		scripts.put(name, factory);

		ConfigDatabase db = conf.ensureDatabase("araqne-logdb-jython");
		ConfigCollection col = db.ensureCollection("scripts");
		Config c = col.findOne(getPredicate(workspace, name));

		if (c == null) {
			LogScriptConfig sc = new LogScriptConfig(workspace, name, script);
			col.add(PrimitiveConverter.serialize(sc));

			// add to logdb script registry
			logScriptRegistry.addScriptFactory(workspace, name, new LogScriptFactoryImpl(factory));
		} else {
			LogScriptConfig sc = new LogScriptConfig(workspace, name, script);
			c.setDocument(PrimitiveConverter.serialize(sc));
			col.update(c);

			logScriptRegistry.removeScriptFactory(workspace, name);
			logScriptRegistry.addScriptFactory(workspace, name, new LogScriptFactoryImpl(factory));
		}
	}

	@Override
	public void removeScript(String workspace, String name) {
		ConfigDatabase db = conf.ensureDatabase("araqne-logdb-jython");
		ConfigCollection col = db.ensureCollection("scripts");
		Config c = col.findOne(Predicates.field("name", name));
		if (c == null)
			throw new IllegalStateException("script not found: " + name);

		col.remove(c);

		// remove from memory
		ConcurrentMap<String, PyObject> scripts = workspaceScripts.get(workspace);
		if (scripts == null)
			return;

		scripts.remove(name);

		// remove from logdb script registry
		logScriptRegistry.removeScriptFactory(workspace, name);
	}

	private PyObject eval(String name, String script) {
		PythonInterpreter interpreter = jython.newInterpreter();
		interpreter.exec("from org.araqne.logdb import LogQueryScript");
		interpreter.exec("from org.araqne.logdb import BaseLogQueryScript");
		interpreter.exec(script);
		PyObject clazz = interpreter.get(name);
		if (clazz == null)
			throw new IllegalStateException("cannot eval jython script " + name);

		return clazz;
	}

	private Predicate getPredicate(String workspace, String name) {
		Map<String, Object> pred = new HashMap<String, Object>();
		pred.put("name", name);
		pred.put("workspace", workspace);
		return Predicates.field(pred);
	}

	private static class LogScriptConfig {
		private String workspace;
		private String name;
		private String script;

		@SuppressWarnings("unused")
		public LogScriptConfig() {
		}

		public LogScriptConfig(String workspace, String name, String script) {
			this.workspace = workspace;
			this.name = name;
			this.script = script;
		}
	}

	private static class LogScriptFactoryImpl implements LogQueryScriptFactory {

		private PyObject factory;

		public LogScriptFactoryImpl(PyObject factory) {
			this.factory = factory;
		}

		@Override
		public LogQueryScript create(Map<String, Object> params) {
			PyObject instance = factory.__call__();
			return (LogQueryScript) instance.__tojava__(LogQueryScript.class);
		}

		@Override
		public String getDescription() {
			PyObject __doc__ = factory.getDoc();
			String doc = (String) __doc__.__tojava__(String.class);
			if (doc == null)
				return "N/A";

			return doc.trim();
		}

		@Override
		public String toString() {
			return getDescription();
		}
	}

}
