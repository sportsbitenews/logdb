package org.araqne.logdb.nashorn.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

import javax.script.Invocable;
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

	@Override
	public NashornParserScript newScript(String scriptName) {
		ScriptEngineManager factory = new ScriptEngineManager();
		ScriptEngine nashornEngine = factory.getEngineByName("nashorn");
		try {
			nashornEngine.eval(new FileReader(new File("d:/test2.txt")));
		} catch (FileNotFoundException e) {
		} catch (ScriptException e) {
		}

		Invocable invocable = (Invocable) nashornEngine;
		return new NashornParserScript(invocable);
	}

}
