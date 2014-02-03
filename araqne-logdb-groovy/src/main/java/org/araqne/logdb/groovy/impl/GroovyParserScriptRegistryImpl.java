package org.araqne.logdb.groovy.impl;

import groovy.util.GroovyScriptEngine;

import java.io.IOException;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logdb.groovy.GroovyParserScript;
import org.araqne.logdb.groovy.GroovyParserScriptRegistry;
import org.osgi.framework.BundleContext;

@Component(name = "logdb-groovy-parser-script-registry")
@Provides
public class GroovyParserScriptRegistryImpl implements GroovyParserScriptRegistry {
	private BundleContext bc;
	private GroovyScriptEngine gse;

	public GroovyParserScriptRegistryImpl(BundleContext bc) {
		this.bc = bc;
	}

	@Validate
	public void start() throws IOException {
		String path = ScriptPaths.getPath("parser_scripts");
		gse = new GroovyScriptEngine(path);
	}

	@Invalidate
	public void stop() {
		if (gse != null)
			gse.getGroovyClassLoader().clearCache();
	}

	@Override
	public GroovyParserScript newScript(String scriptName) {
		try {
			Class<?> clazz = gse.loadScriptByName(scriptName + ".groovy");
			Object o = clazz.newInstance();
			GroovyParserScript script = (GroovyParserScript) o;
			script.setBundleContext(bc);
			return script;
		} catch (Throwable t) {
			throw new IllegalStateException("cannot instanciate groovy parser script: " + scriptName, t);
		}
	}

}
