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

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logdb.groovy.GroovyQueryScript;
import org.araqne.logdb.groovy.GroovyQueryScriptRegistry;
import org.osgi.framework.BundleContext;

@Component(name = "logdb-groovy-query-script-registry")
@Provides
public class GroovyQueryScriptRegistryImpl implements GroovyQueryScriptRegistry {
	private BundleContext bc;
	private GroovyScriptEngine gse;

	public GroovyQueryScriptRegistryImpl(BundleContext bc) {
		this.bc = bc;
	}

	@Validate
	public void start() throws IOException {
		String path = ScriptPaths.getPath("query_scripts");
		gse = new GroovyScriptEngine(path);
	}

	@Invalidate
	public void stop() {
		if (gse != null)
			gse.getGroovyClassLoader().clearCache();
	}

	@Override
	public GroovyQueryScript newScript(String scriptName) {
		try {
			Class<?> clazz = gse.loadScriptByName(scriptName + ".groovy");
			Object o = clazz.newInstance();
			GroovyQueryScript script = (GroovyQueryScript) o;
			script.setBundleContext(bc);
			return script;
		} catch (Throwable t) {
			throw new IllegalStateException("cannot instanciate groovy query script: " + scriptName, t);
		}
	}
}
