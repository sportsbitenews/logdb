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
import org.araqne.logdb.nashorn.NashornParserScript;
import org.araqne.logdb.nashorn.NashornParserScriptRegistry;

@Component(name = "nashorn-parser-script-registry")
@Provides
public class NashornParserScriptRegistryImpl implements NashornParserScriptRegistry {
	private ScriptEngineManager factory = new ScriptEngineManager();
	private File scriptDir;

	public NashornParserScriptRegistryImpl() {
		this.scriptDir = ScriptPaths.getPath("parser_scripts");
	}

	@Override
	public NashornParserScript newScript(String scriptName) {
		// TODO: need to cache script file
		ScriptEngine nashornEngine = factory.getEngineByName("nashorn");
		File file = new File(scriptDir, scriptName + ".js");
		try {
			nashornEngine.eval(new FileReader(file));
		} catch (FileNotFoundException e) {
			throw new IllegalStateException("javascript parser script not found: " + file.getAbsolutePath(), e);
		} catch (ScriptException e) {
			throw new IllegalStateException("cannot eval parser script: " + file.getAbsolutePath(), e);
		}

		return new NashornParserScript(scriptName, nashornEngine);
	}

}
