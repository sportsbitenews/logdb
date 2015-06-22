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
import java.io.FileNotFoundException;
import java.io.FileReader;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.araqne.logdb.nashorn.NashornQueryScript;
import org.araqne.logdb.nashorn.NashornQueryScriptRegistry;
import org.osgi.framework.BundleContext;

@Component(name = "logdb-nashorn-query-script-registry")
@Provides
public class NashornQueryScriptRegistryImpl implements NashornQueryScriptRegistry {
	private ScriptEngineManager factory;
	private File scriptDir;
	private BundleContext bc;

	public NashornQueryScriptRegistryImpl(BundleContext bc) {
		this.bc = bc;
		this.scriptDir = ScriptPaths.getPath("query_scripts");
		this.factory = new ScriptEngineManager();
	}

	@Override
	public NashornQueryScript newScript(String scriptName) {
		ClassLoader oldLoader = Thread.currentThread().getContextClassLoader();
		File file = new File(scriptDir, scriptName + ".js");
		String path = file.getAbsolutePath();

		try {
			Thread.currentThread().setContextClassLoader(NashornQueryScript.class.getClassLoader());
			ScriptEngine nashornEngine = factory.getEngineByName("nashorn");
			if (nashornEngine == null)
				throw new IllegalStateException("cannot load nashorn engine for javascript " + scriptName);

			nashornEngine.eval("var QueryScript = Java.type(\"org.araqne.logdb.nashorn.NashornQueryScript\");");
			nashornEngine.eval(new FileReader(file));
			NashornQueryScript script = (NashornQueryScript) nashornEngine.eval("new " + scriptName + "();");
			script.setBundleContext(bc);
			return script;
		} catch (FileNotFoundException e) {
			throw new IllegalStateException("javascript parser script not found: " + path, e);
		} catch (ScriptException e) {
			throw new IllegalStateException("cannot eval parser script: " + path + ", cause: " + e.toString(), e);
		} finally {
			Thread.currentThread().setContextClassLoader(oldLoader);
		}
	}

}
