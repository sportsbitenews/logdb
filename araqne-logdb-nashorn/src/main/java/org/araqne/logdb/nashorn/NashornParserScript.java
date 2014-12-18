package org.araqne.logdb.nashorn;

import java.util.List;
import java.util.Map;

import javax.script.Invocable;
import javax.script.ScriptException;

import org.araqne.log.api.FieldDefinition;
import org.araqne.log.api.LogParser;
import org.araqne.log.api.LogParserInput;
import org.araqne.log.api.LogParserOutput;

public class NashornParserScript implements LogParser {
	private Invocable invocable;

	public NashornParserScript(Invocable invocable) {
		this.invocable = invocable;
	}

	@Override
	public int getVersion() {
		return 1;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		try {
			return (Map<String, Object>) invocable.invokeFunction("parse", params);
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
