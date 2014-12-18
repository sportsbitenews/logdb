package org.araqne.logdb.nashorn.impl;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.araqne.api.Script;
import org.araqne.api.ScriptContext;

public class NashornScript implements Script {
	private ScriptContext context;

	@Override
	public void setScriptContext(ScriptContext context) {
		this.context = context;
	}

	public void hello(String[] args) throws ScriptException, NoSuchMethodException {
		ScriptEngineManager factory = new ScriptEngineManager();
		ScriptEngine nashornEngine = factory.getEngineByName("nashorn");
		nashornEngine.eval("function hello(name) { return 'hello ' + name; }");
		Invocable invocable = (Invocable) nashornEngine;
		Object result = invocable.invokeFunction("hello", args[0]);
		context.println(result);
	}

}
