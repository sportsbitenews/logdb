package org.araqne.logdb.groovy;

import java.util.List;
import java.util.Map;

import org.araqne.log.api.FieldDefinition;
import org.araqne.log.api.LogParser;
import org.araqne.log.api.LogParserInput;
import org.araqne.log.api.LogParserOutput;
import org.osgi.framework.BundleContext;

public class GroovyParserScript implements LogParser {
	protected BundleContext bc;

	public void setBundleContext(BundleContext bc) {
		this.bc = bc;
	}

	@Override
	public int getVersion() {
		return 1;
	}

	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		throw new UnsupportedOperationException("not implemented");
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
