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
import java.util.Collection;
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
import org.araqne.log.api.LogParser;
import org.araqne.log.api.LogParserFactory;
import org.araqne.log.api.LoggerConfigOption;
import org.araqne.log.api.StringConfigType;
import org.araqne.log.api.V1LogParser;
import org.araqne.logdb.jython.JythonParserScriptRegistry;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

@Component(name = "jython-parser-script-registry")
@Provides
public class JythonParserScriptRegistryImpl implements JythonParserScriptRegistry, LogParserFactory {
	private final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JythonParserScriptRegistryImpl.class);
	private ConcurrentMap<String, PyObject> scripts = new ConcurrentHashMap<String, PyObject>();

	@Requires
	private JythonService jython;

	@Requires
	private ConfigService conf;

	@Validate
	public void start() {
		// load scripts
		ConfigDatabase db = conf.ensureDatabase("araqne-logdb-jython");
		ConfigIterator it = db.find(ScriptConfig.class, Predicates.field("type", "parser"));
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

	@Override
	public String getName() {
		return "jython";
	}

	@Override
	public List<Locale> getLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.CHINESE);
	}

	@Override
	public String getDisplayGroup(Locale locale) {
		return "General";
	}

	@Override
	public Collection<Locale> getDisplayNameLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.CHINESE);
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale != null && locale.equals(Locale.CHINESE))
			return "jython脚本解析器";
		return "jython parser";

	}

	@Override
	public Collection<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.CHINESE);
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.CHINESE))
			return "jython脚本解析器";
		return "jython parser script factory";
	}

	@Override
	public Collection<LoggerConfigOption> getConfigOptions() {
		LoggerConfigOption scriptName = new StringConfigType("parser_script", map("parser script name"),
				map("jython parser script class name"), true);
		return Arrays.asList(scriptName);
	}

	private Map<Locale, String> map(String value) {
		Map<Locale, String> m = new HashMap<Locale, String>();
		m.put(Locale.ENGLISH, value);
		return m;
	}

	@Override
	public LogParser createParser(Map<String, String> config) {
		String scriptName = (String) config.get("parser_script");
		PyObject factory = scripts.get(scriptName);
		if (factory == null)
			return null;

		PyObject instance = factory.__call__();
		return new WrapLogParser((LogParser) instance.__tojava__(LogParser.class));
	}

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
		pred.put("type", "parser");
		return Predicates.field(pred);
	}

	@Override
	public void loadScript(String name, String script) {
		PyObject factory = eval(name, script);
		scripts.put(name, factory);

		ScriptConfig sc = new ScriptConfig(name, "parser", script);
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
		interpreter.exec("from org.araqne.log.api import LogParser");
		interpreter.exec("from java.util import Date");
		interpreter.exec(script);
		PyObject clazz = interpreter.get(name);
		if (clazz == null)
			throw new IllegalStateException("cannot eval jython parser script " + name);

		return clazz;
	}

	private class WrapLogParser extends V1LogParser {
		private LogParser delegation;

		public WrapLogParser(LogParser delegation) {
			this.delegation = delegation;
		}

		@Override
		public Map<String, Object> parse(Map<String, Object> params) {
			Map<String, Object> m = delegation.parse(params);
			if (logger.isDebugEnabled())
				logger.debug("araqne logdb jython: parsed map [{}]", m);
			return m;
		}

	}
}
