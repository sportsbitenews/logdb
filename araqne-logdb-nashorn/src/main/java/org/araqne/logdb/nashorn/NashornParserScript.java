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
package org.araqne.logdb.nashorn;

import java.util.List;
import java.util.Map;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

import org.araqne.log.api.FieldDefinition;
import org.araqne.log.api.LogParser;
import org.araqne.log.api.LogParserInput;
import org.araqne.log.api.LogParserOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NashornParserScript implements LogParser {
	private final Logger slog = LoggerFactory.getLogger(NashornParserScript.class);
	private String className;
	private ScriptEngine engine;
	private Object instance;
	private boolean suppressError;

	public NashornParserScript(String className, ScriptEngine engine) {
		this.className = className;
		this.engine = engine;
	}

	@Override
	public int getVersion() {
		return 1;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		if (instance == null) {
			try {
				instance = engine.eval("new " + className + "()");
			} catch (ScriptException e) {
				if (!suppressError) {
					slog.error("araqne logdb nashorn: cannot instanciate javascript parser " + className, e);
					suppressError = true;
				}
			}
		}

		try {
			Invocable invocable = (Invocable) engine;
			return (Map<String, Object>) invocable.invokeMethod(instance, "parse", params);
		} catch (ScriptException e) {
			e.printStackTrace();
			return null;
		} catch (NoSuchMethodException e) {
			return null;
		}
	}

	@Override
	public LogParserOutput parse(LogParserInput input) {
		throw new UnsupportedOperationException("not implemented");
	}

	@Override
	public List<FieldDefinition> getFieldDefinitions() {
		return null;
	}

}
