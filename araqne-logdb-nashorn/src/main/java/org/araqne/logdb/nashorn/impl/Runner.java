package org.araqne.logdb.nashorn.impl;

import java.io.File;
import java.io.FileReader;
import java.util.HashMap;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public class Runner {
	public static void main(String[] args) throws Exception {
		HashMap<String, Object> m = new HashMap<String, Object>();
		m.put("line", "hello;world");

		ScriptEngineManager factory = new ScriptEngineManager();
		ScriptEngine nashornEngine = factory.getEngineByName("nashorn");
		System.out.println(nashornEngine);
		nashornEngine.eval(new FileReader(new File("d:/test2.txt")));
		Invocable invocable = (Invocable) nashornEngine;

		Object result = invocable.invokeFunction("parse", m);
		System.out.println(result);
		System.out.println(m);
	}

}
