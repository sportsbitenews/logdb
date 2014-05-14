/*
 * Copyright 2013 Eediom Inc.
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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.confdb.Config;
import org.araqne.confdb.ConfigDatabase;
import org.araqne.confdb.ConfigIterator;
import org.araqne.confdb.ConfigService;
import org.araqne.confdb.Predicate;
import org.araqne.confdb.Predicates;
import org.araqne.jython.JythonService;
import org.araqne.log.api.LogTransformer;
import org.araqne.log.api.LogTransformerFactory;
import org.araqne.log.api.LoggerConfigOption;
import org.araqne.log.api.StringConfigType;
import org.araqne.logdb.jython.JythonTransformerScriptRegistry;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

@Component(name = "jython-transformer-script-registry")
@Provides
public class JythonTransformerScriptRegistryImpl implements JythonTransformerScriptRegistry, LogTransformerFactory {
	private final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JythonTransformerScriptRegistryImpl.class);
	private ConcurrentMap<String, PyObject> scripts = new ConcurrentHashMap<String, PyObject>();

	@Requires
	private JythonService jython;

	@Requires
	private ConfigService conf;

	@Validate
	public void start() {
		// load scripts
		ConfigDatabase db = conf.ensureDatabase("araqne-logdb-jython");
		ConfigIterator it = db.find(ScriptConfig.class, Predicates.field("type", "transformer"));
		try {
			for (ScriptConfig sc : it.getDocuments(ScriptConfig.class)) {
				try {
					PyObject o = eval(sc.getName(), sc.getScript());
					scripts.put(sc.getName(), o);
				} catch (Throwable t) {
					logger.error("araqne logdb jython: cannot load logger script [" + sc.getName() + "]", t);
				}
			}
		} finally {
			it.close();
		}

	}

	@Invalidate
	public void stop() {
	}

	//
	// implement transformer factory
	//

	@Override
	public String getName() {
		return "jython";
	}

	@Override
	public String getDisplayName(Locale locale) {
		
		return "jython logger";
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.CHINESE))
			return "jython logger脚本";
		return "jython logger script";
	}

	@Override
	public List<Locale> getDisplayNameLocales() {
		return Arrays.asList(Locale.ENGLISH);
	}

	@Override
	public List<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.CHINESE);
	}

	@Override
	public List<LoggerConfigOption> getConfigOptions() {
		LoggerConfigOption scriptName = new StringConfigType("transformer_script", map("transformer script name", "transformer脚本名称"),
				map("jython transformer script class name", "jython transformer脚本类名称"), true);
		return Arrays.asList(scriptName);
	}

	private Map<Locale, String> map(String value, String cn) {
		Map<Locale, String> m = new HashMap<Locale, String>();
		m.put(Locale.ENGLISH, value);
		m.put(Locale.CHINESE, cn);
		return m;
	}

	@Override
	public LogTransformer newTransformer(Map<String, String> config) {
		String scriptName = (String) config.get("transformer_script");
		PyObject factory = scripts.get(scriptName);
		if (factory == null)
			return null;

		PyObject instance = factory.__call__();
		return (LogTransformer) instance.__tojava__(LogTransformer.class);
	}

	//
	// implement script registry
	//

	@Override
	public Set<String> getScriptNames() {
		return scripts.keySet();
	}

	@Override
	public String getScriptCode(String name) {
		ConfigDatabase db = conf.ensureDatabase("araqne-logdb-jython");
		Config c = db.findOne(ScriptConfig.class, getPredicate(name));
		if (c == null)
			return null;

		ScriptConfig sc = c.getDocument(ScriptConfig.class);
		return sc.getScript();
	}

	private Predicate getPredicate(String name) {
		Map<String, Object> pred = new HashMap<String, Object>();
		pred.put("name", name);
		pred.put("type", "transformer");
		return Predicates.field(pred);
	}

	@Override
	public void loadScript(String name, String script) {
		PyObject factory = eval(name, script);
		scripts.put(name, factory);

		ScriptConfig sc = new ScriptConfig(name, "transformer", script);
		ConfigDatabase db = conf.ensureDatabase("araqne-logdb-jython");
		Config c = db.findOne(ScriptConfig.class, getPredicate(name));
		if (c == null) {
			db.add(sc);
		} else {
			db.update(c, sc);
		}
	}

	@Override
	public void unloadScript(String name) {
		ConfigDatabase db = conf.ensureDatabase("araqne-logdb-jython");
		Config c = db.findOne(ScriptConfig.class, Predicates.field("name", name));
		if (c == null)
			throw new IllegalStateException("script not found: " + name);

		scripts.remove(name);
	}

	private PyObject eval(String name, String script) {
		PythonInterpreter interpreter = jython.newInterpreter();
		interpreter.exec("from org.araqne.log.api import SimpleLog");
		interpreter.exec("from org.araqne.log.api import LogTransformer");
		interpreter.exec("from java.util import Date");
		interpreter.exec(script);
		PyObject clazz = interpreter.get(name);
		if (clazz == null)
			throw new IllegalStateException("cannot eval jython logger script " + name);

		return clazz;
	}
}
